# ezl/core.py
"""
Minimal ezl core with `match` usage and compact OpenAPI support.

Assumptions:
- Task functions are one of: sync, sync_gen, async, async_gen.
- We classify function kind once at decoration time.
- Workers use blocking downstream.put() for deterministic backpressure.
- Webhook start will inject model schema into OpenAPI for single + batch endpoints.
"""

import asyncio
import inspect
import logging
import queue
import threading
import time
from collections import defaultdict, deque
from typing import Any, Callable, Dict, List, Optional, Type

from fastapi import FastAPI, Request
from fastapi.openapi.utils import get_openapi
from fastapi.responses import (
    JSONResponse,
    PlainTextResponse,
)
import uvicorn
from pydantic import BaseModel, ValidationError

# small custom level for flow tracing
FLOW_LEVEL = 25
logging.addLevelName(FLOW_LEVEL, "FLOW")


def _flow(self, msg, *a, **k):
    if self.isEnabledFor(FLOW_LEVEL):
        self._log(FLOW_LEVEL, msg, a, **k)


logging.Logger.flow = _flow
logger = logging.getLogger(__name__)


def get_item_key(item: Any, key: str) -> str:
    keys = key.split(".")
    val = item
    for k in keys:
        val = val[k]
    return str(val)


class Task:
    """A node in the pipeline. Uses match/case for clarity."""

    def __init__(
        self, func: Callable, buffer: int, workers: int
    ):
        self.func = func
        self.name = getattr(func, "__name__", "task")
        self.buffer = max(1, buffer)
        self.workers = max(0, workers)
        self.upstream: List["Task"] = []
        self.downstream: Optional["Task"] = None
        self.input_queue: queue.Queue = queue.Queue(
            maxsize=self.buffer
        )
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []

        # classify function kind once
        if inspect.isasyncgenfunction(func):
            self.kind = "async_gen"
        elif inspect.iscoroutinefunction(func):
            self.kind = "async"
        elif inspect.isgeneratorfunction(func):
            self.kind = "sync_gen"
        else:
            self.kind = "sync"

        # webhook-related
        self.is_webhook = False
        self.webhook_conf: Dict[str, Any] = {}
        self._webhook_server = None
        self._webhook_thread: Optional[threading.Thread] = (
            None
        )

        # collector-related
        self.is_collector = False
        self.log_file: Optional[str] = None
        self.log_key: Optional[str] = None
        self.filter_key: Optional[str] = None

    def __rshift__(self, other: "Task"):
        if not isinstance(other, Task):
            return NotImplemented
        return Pipeline(self, other)

    def __or__(self, other: "Task"):
        if not isinstance(other, Task):
            return NotImplemented
        return ChoiceTask(self, other, condition=None)

    # -------------------------
    # worker lifecycle
    # -------------------------
    def start_workers(self):
        if not self.upstream:
            logger.debug(
                f"'{self.name}' has no upstream -> no workers"
            )
            return
        logger.info(
            f"Starting {self.workers} worker(s) for '{self.name}'"
        )
        for i in range(self.workers):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"{self.name}-{i}",
                daemon=True,
            )
            t.start()
            self._threads.append(t)

    def _worker_loop(self):
        logger.debug(
            f"Worker {threading.current_thread().name} starting ({self.kind})"
        )
        try:
            match self.kind:
                case "async" | "async_gen":
                    asyncio.run(self._async_worker())
                case "choice":
                    asyncio.run(self._choice_worker())
                case _:
                    self._sync_worker()
        finally:
            logger.debug(
                f"Worker {threading.current_thread().name} exiting"
            )

    def _sync_worker(self):
        while not self._stop.is_set():
            try:
                item = self.input_queue.get(timeout=0.5)
                logger.flow(f"PULL [{self.name}]")
                try:
                    match self.kind:
                        case "sync_gen":
                            for out in self.func(item):
                                logger.flow(
                                    f"PROC [{self.name}]"
                                )
                                self._send_downstream(out)
                        case "sync":
                            out = self.func(item)
                            if out is not None:
                                logger.flow(
                                    f"PROC [{self.name}]"
                                )
                                self._send_downstream(out)
                        case _:
                            # should not happen
                            logger.error(
                                f"Unexpected kind in sync worker: {self.kind}"
                            )
                except Exception:
                    logger.exception(
                        f"Error in '{self.name}'"
                    )
                finally:
                    self.input_queue.task_done()
            except queue.Empty:
                continue

    async def _async_worker(self):
        while not self._stop.is_set():
            try:
                item = self.input_queue.get(timeout=0.5)
                logger.flow(f"PULL [{self.name}]")
                try:
                    match self.kind:
                        case "async_gen":
                            agen = self.func(item)
                            async for out in agen:
                                logger.flow(
                                    f"PROC [{self.name}]"
                                )
                                self._send_downstream(out)
                        case "async":
                            out = await self.func(item)
                            if out is not None:
                                logger.flow(
                                    f"PROC [{self.name}]"
                                )
                                self._send_downstream(out)
                        case _:
                            logger.error(
                                f"Unexpected kind in async worker: {self.kind}"
                            )
                except Exception:
                    logger.exception(
                        f"Error in async '{self.name}'"
                    )
                finally:
                    self.input_queue.task_done()
            except queue.Empty:
                await asyncio.sleep(0.01)
                continue

    async def _choice_worker(self):
        while not self._stop.is_set():
            try:
                item = self.input_queue.get(timeout=0.5)
                logger.flow(f"PULL [{self.name}]")
                try:
                    if self.condition is None:
                        raise ValueError(
                            f"Condition not set for choice task '{self.name}'"
                        )
                    use_left = self.condition(item)
                    branch = (
                        self.left
                        if use_left
                        else self.right
                    )
                    branch_name = branch.name
                    match branch.kind:
                        case "sync_gen":
                            for out in branch.func(item):
                                logger.flow(
                                    f"PROC [{self.name}:{branch_name}]"
                                )
                                self._send_downstream(out)
                        case "sync":
                            out = branch.func(item)
                            if out is not None:
                                logger.flow(
                                    f"PROC [{self.name}:{branch_name}]"
                                )
                                self._send_downstream(out)
                        case "async_gen":
                            agen = branch.func(item)
                            async for out in agen:
                                logger.flow(
                                    f"PROC [{self.name}:{branch_name}]"
                                )
                                self._send_downstream(out)
                        case "async":
                            out = await branch.func(item)
                            if out is not None:
                                logger.flow(
                                    f"PROC [{self.name}:{branch_name}]"
                                )
                                self._send_downstream(out)
                except Exception:
                    logger.exception(
                        f"Error in choice '{self.name}'"
                    )
                finally:
                    self.input_queue.task_done()
            except queue.Empty:
                await asyncio.sleep(0.01)
                continue

    def _send_downstream(self, item: Any):
        if not self.downstream:
            logger.debug(
                f"'{self.name}' sink processed item"
            )
            return
        logger.flow(
            f"PUT  [{self.name}] -> [{self.downstream.name}]"
        )
        try:
            # blocking put for deterministic backpressure
            self.downstream.input_queue.put(
                item, block=True
            )
        except Exception:
            logger.exception(
                f"Failed to put into downstream '{self.downstream.name}'"
            )

    def wait_for_queue(self):
        if self.upstream:
            self.input_queue.join()

    def signal_stop(self):
        self._stop.set()
        if (
            self.is_webhook
            and self._webhook_server is not None
        ):
            try:
                self._webhook_server.should_exit = True
            except Exception:
                logger.exception(
                    "Error stopping webhook server"
                )

    def stop_gracefully(self):
        self.signal_stop()
        for t in self._threads:
            t.join(timeout=2)
        if self.is_webhook and self._webhook_thread:
            self._webhook_thread.join(timeout=3)

    # -------------------------
    # webhook + OpenAPI
    # -------------------------
    def _build_fastapi(
        self,
        path: str,
        *,
        api_key: Optional[str],
        model: Optional[Type[BaseModel]],
        rate_limit_per_minute: Optional[int],
    ):
        app = FastAPI(title=f"ezl:webhook:{self.name}")

        window = 60
        if rate_limit_per_minute:
            counters: Dict[str, deque] = defaultdict(deque)
            counters_lock = asyncio.Lock()
        else:
            counters = None
            counters_lock = None

        @app.post(path, summary="ingest single")
        async def single(request: Request):
            if request.method != "POST":
                return PlainTextResponse(
                    "Method not allowed", status_code=405
                )
            client_ip = (
                request.client.host
                if request.client
                else "unknown"
            )
            identity = client_ip

            if api_key is not None:
                incoming = request.headers.get(
                    "x-api-key"
                ) or request.headers.get("X-API-Key")
                if incoming != api_key:
                    logger.flow(
                        f"AUTH FAIL [{self.name}] ip={client_ip}"
                    )
                    return JSONResponse(
                        {"error": "Unauthorized"},
                        status_code=401,
                    )
                identity = f"api_key:{incoming}"

            if counters is not None:
                now = time.time()
                async with counters_lock:
                    dq = counters[identity]
                    while dq and dq[0] <= now - window:
                        dq.popleft()
                    if len(dq) >= rate_limit_per_minute:
                        logger.flow(
                            f"RATE LIMIT [{self.name}] id={identity} count={len(dq)}"
                        )
                        return JSONResponse(
                            {
                                "error": "Rate limit exceeded"
                            },
                            status_code=429,
                        )
                    dq.append(now)

            try:
                payload = await request.json()
            except Exception:
                return JSONResponse(
                    {"error": "Invalid JSON"},
                    status_code=400,
                )

            if isinstance(payload, list):
                return JSONResponse(
                    {"error": "Use /batch for arrays"},
                    status_code=400,
                )

            if model is not None:
                try:
                    validated = model.model_validate(
                        payload
                    )
                    obj = validated.model_dump()
                except ValidationError:
                    return JSONResponse(
                        {"accepted": 0, "rejected": 1},
                        status_code=422,
                    )
            else:
                obj = payload

            if self.downstream:
                try:
                    self.downstream.input_queue.put(
                        obj, block=False
                    )
                except queue.Full:
                    logger.flow(
                        f"WEBHOOK [{self.name}] queue full -> rejecting"
                    )
                    return JSONResponse(
                        {
                            "accepted": 0,
                            "rejected": 1,
                            "error": "Queue full",
                        },
                        status_code=503,
                        headers={"Retry-After": "10"},
                    )
            else:
                logger.warning(
                    f"Webhook '{self.name}' has no downstream"
                )

            logger.flow(f"WEBHOOK [{self.name}] accepted=1")
            return JSONResponse(
                {"accepted": 1, "rejected": 0},
                status_code=202,
            )

        # batch route if model present
        if model is not None:
            batch_path = path.rstrip("/") + "/batch"

            async def batch(items: list[model]):  # type: ignore
                accepted = 0
                rejected = 0
                for inst in items:
                    try:
                        obj = inst.model_dump()
                        if self.downstream:
                            self.downstream.input_queue.put(
                                obj, block=False
                            )
                            accepted += 1
                    except queue.Full:
                        logger.flow(
                            f"WEBHOOK [{self.name}] queue full during batch -> accepted={accepted}"
                        )
                        return JSONResponse(
                            {
                                "accepted": accepted,
                                "rejected": len(items)
                                - accepted,
                                "error": "Queue full",
                            },
                            status_code=503,
                        )
                    except Exception:
                        rejected += 1
                logger.flow(
                    f"WEBHOOK [{self.name}] batch accepted={accepted} rejected={rejected}"
                )
                return JSONResponse(
                    {
                        "accepted": accepted,
                        "rejected": rejected,
                    },
                    status_code=202,
                )

            app.post(batch_path, summary="ingest batch")(
                batch
            )

            # inject model schema for nicer OpenAPI docs
            def custom_openapi():
                if app.openapi_schema:
                    return app.openapi_schema
                schema = get_openapi(
                    title=app.title,
                    version="1.0.0",
                    routes=app.routes,
                )
                try:
                    model_schema = model.model_json_schema(
                        ref_template="#/components/schemas/{model}"
                    )
                except Exception:
                    model_schema = model.model_json_schema()
                comp = schema.setdefault(
                    "components", {}
                ).setdefault("schemas", {})
                comp.setdefault(
                    model.__name__, model_schema
                )

                # annotate single endpoint requestBody
                if path in schema.get("paths", {}):
                    post = schema["paths"][path].get("post")
                    if post:
                        post["requestBody"] = {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": f"#/components/schemas/{model.__name__}"
                                    }
                                }
                            },
                            "required": True,
                        }

                # annotate batch endpoint
                bp = batch_path
                if bp in schema.get("paths", {}):
                    post = schema["paths"][bp].get("post")
                    if post:
                        post["requestBody"] = {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "$ref": f"#/components/schemas/{model.__name__}"
                                        },
                                    }
                                }
                            },
                            "required": True,
                        }

                app.openapi_schema = schema
                return schema

            app.openapi = custom_openapi

        return app

    def start_webhook(
        self,
        *,
        host: str = "0.0.0.0",
        port: int = 8000,
        path: str = "/",
        api_key: Optional[str] = None,
        model: Optional[Type[BaseModel]] = None,
        rate_limit_per_minute: Optional[int] = None,
    ):
        if not self.is_webhook:
            raise RuntimeError(
                "start_webhook on non-webhook task"
            )
        app = self._build_fastapi(
            path,
            api_key=api_key,
            model=model,
            rate_limit_per_minute=rate_limit_per_minute,
        )
        cfg = uvicorn.Config(
            app, host=host, port=port, log_level="info"
        )
        server = uvicorn.Server(config=cfg)

        def _serve():
            logger.info(
                f"Starting webhook {self.name} at http://{host}:{port}{path}"
            )
            server.run()
            logger.info("Webhook stopped")

        thr = threading.Thread(
            target=_serve,
            name=f"webhook-{self.name}",
            daemon=True,
        )
        thr.start()
        self._webhook_server = server
        self._webhook_thread = thr


class ChoiceTask(Task):
    def __init__(
        self,
        left: Task,
        right: Task,
        condition: Optional[Callable],
    ):
        buffer = max(left.buffer, right.buffer)
        workers = max(left.workers, right.workers)
        super().__init__(
            func=None, buffer=buffer, workers=workers
        )
        self.kind = "choice"
        self.left = left
        self.right = right
        self.condition = condition
        self.name = f"choice({left.name}|{right.name})"

    def __xor__(self, other: Callable):
        if not callable(other):
            raise TypeError(
                "^ expects a callable condition"
            )
        self.condition = other
        return self

    def __or__(self, other: "Task"):
        if not isinstance(other, Task):
            return NotImplemented
        return ChoiceTask(self, other, condition=None)


class Pipeline:
    """Tiny pipeline container and runner using match/case."""

    def __init__(self, a: Task, b: Task):
        a.downstream = b
        b.upstream.append(a)
        self.tasks: List[Task] = [a, b]
        self.source = a
        self.sink = b

    def __rshift__(self, other: "Task"):
        if not isinstance(other, Task):
            return NotImplemented
        self.sink.downstream = other
        other.upstream.append(self.sink)
        self.tasks.append(other)
        self.sink = other
        return self

    def __or__(self, other: "Task"):
        return self.sink.__or__(other)

    def __xor__(self, other: Callable):
        if isinstance(self.sink, ChoiceTask):
            return self.sink.__xor__(other)
        raise TypeError("^ can only be used after |")

    def _run_source_sync(self, src: Task):
        logger.info(f"Running source '{src.name}' (sync)")
        try:
            match src.kind:
                case "sync_gen":
                    for it in src.func():
                        src._send_downstream(it)
                case "sync":
                    out = src.func()
                    if out is not None:
                        src._send_downstream(out)
                case _:
                    logger.error(
                        f"Unexpected source kind: {src.kind}"
                    )
        except Exception:
            logger.exception(f"Source '{src.name}' failed")

    async def _run_source_async(self, src: Task):
        logger.info(f"Running source '{src.name}' (async)")
        try:
            match src.kind:
                case "async_gen":
                    async for it in src.func():
                        src._send_downstream(it)
                case "async":
                    out = await src.func()
                    if out is not None:
                        src._send_downstream(out)
                case _:
                    logger.error(
                        f"Unexpected async source kind: {src.kind}"
                    )
        except Exception:
            logger.exception(
                f"Async source '{src.name}' failed"
            )

    def run(
        self,
        log_level: int = logging.INFO,
        resume: bool = False,
    ):
        logging.basicConfig(
            level=log_level,
            format="%(levelname)-8s | %(message)s",
        )
        logger.setLevel(log_level)
        logger.info("🚀 Pipeline starting")

        if resume and self.sink.is_collector:
            processed = set()
            try:
                with open(self.sink.log_file, "r") as f:
                    for line in f:
                        processed.add(line.strip())
            except FileNotFoundError:
                pass

            original_func = self.source.func
            kind = self.source.kind
            filter_key = self.sink.filter_key

            def get_key(item):
                return get_item_key(item, filter_key)

            if kind == "sync_gen":

                def wrapped():
                    for item in original_func():
                        if get_key(item) in processed:
                            continue
                        yield item

                self.source.func = wrapped
            elif kind == "async_gen":

                async def wrapped():
                    async for item in original_func():
                        if get_key(item) in processed:
                            continue
                        yield item

                self.source.func = wrapped
            elif kind == "sync":

                def wrapped():
                    item = original_func()
                    if (
                        item is not None
                        and get_key(item) in processed
                    ):
                        return None
                    return item

                self.source.func = wrapped
            elif kind == "async":

                async def wrapped():
                    item = await original_func()
                    if (
                        item is not None
                        and get_key(item) in processed
                    ):
                        return None
                    return item

                self.source.func = wrapped

        for t in self.tasks:
            t.start_workers()

        source_tasks = [
            t for t in self.tasks if not t.upstream
        ]
        threads: List[threading.Thread] = []

        for s in source_tasks:
            if getattr(s, "is_webhook", False):
                conf = s.webhook_conf
                s.start_webhook(
                    host=conf.get("host", "0.0.0.0"),
                    port=conf.get("port", 8000),
                    path=conf.get("path", "/"),
                    api_key=conf.get("api_key"),
                    model=conf.get("model"),
                    rate_limit_per_minute=conf.get(
                        "rate_limit_per_minute"
                    ),
                )
                if s._webhook_thread:
                    threads.append(s._webhook_thread)
                continue

            match s.kind:
                case "async" | "async_gen":
                    thr = threading.Thread(
                        target=lambda ss=s: asyncio.run(
                            self._run_source_async(ss)
                        ),
                        daemon=True,
                    )
                case _:
                    thr = threading.Thread(
                        target=lambda ss=s: self._run_source_sync(
                            ss
                        ),
                        daemon=True,
                    )
            thr.start()
            threads.append(thr)

        # wait non-webhook sources
        non_webhooks = [
            th
            for th in threads
            if not th.name.startswith("webhook-")
        ]
        for th in non_webhooks:
            th.join()

        if any(t.is_webhook for t in self.tasks):
            logger.info(
                "Webhook(s) running. Ctrl+C to stop."
            )
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Shutdown requested")
        else:
            logger.info(
                "Sources finished; draining queues..."
            )

        for t in self.tasks:
            t.wait_for_queue()

        logger.info("Shutting down workers...")
        for t in self.tasks:
            t.signal_stop()
        for t in self.tasks:
            t.stop_gracefully()

        logger.info("✅ Pipeline finished")


# -----------------------
# decorators preserved
# -----------------------
def task(buffer: int = 100, workers: int = 3):
    def dec(fn: Callable) -> Task:
        return Task(fn, buffer=buffer, workers=workers)

    return dec


def webhook(
    path: str = "/",
    host: str = "0.0.0.0",
    port: int = 8000,
    buffer: int = 100,
    api_key: Optional[str] = None,
    model: Optional[Type[BaseModel]] = None,
    rate_limit_per_minute: Optional[int] = None,
):
    def dec(fn: Callable) -> Task:
        t = Task(fn, buffer=buffer, workers=0)
        t.is_webhook = True
        t.webhook_conf = {
            "path": path,
            "host": host,
            "port": port,
            "api_key": api_key,
            "model": model,
            "rate_limit_per_minute": rate_limit_per_minute,
        }
        return t

    return dec


def collector(
    log_file: str,
    key: str = "id",
    filter_key: Optional[str] = None,
    buffer: int = 100,
    workers: int = 1,
):
    def log_func(item):
        val = get_item_key(item, key)
        with open(log_file, "a") as f:
            f.write(val + "\n")

    t = Task(log_func, buffer=buffer, workers=workers)
    t.is_collector = True
    t.log_file = log_file
    t.log_key = key
    t.filter_key = filter_key or key
    return t


def run(
    p: Pipeline,
    log_level: int = logging.INFO,
    resume: bool = False,
):
    p.run(log_level=log_level, resume=resume)
