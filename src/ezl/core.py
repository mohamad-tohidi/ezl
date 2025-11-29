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
from multiprocessing import (
    Process,
    Queue as MPQueue,
    Event as MPEvent,
)

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

SENTINEL = object()


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
        self.input_queue: MPQueue = MPQueue(
            maxsize=self.buffer
        )
        self._stop = MPEvent()
        self._threads: List[threading.Thread] = []
        self._sentinels_received: int = 0
        self._sentinel_lock = threading.Lock()

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
        self._process = None

    def __rshift__(self, other: "Task"):
        if not isinstance(other, Task):
            return NotImplemented
        return Pipeline(self, other)

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
                if item is SENTINEL:
                    with self._sentinel_lock:
                        self._sentinels_received += 1
                        if self._sentinels_received == len(
                            self.upstream
                        ):
                            self._send_sentinel_downstream()
                            self._stop.set()
                    continue
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
            except queue.Empty:
                continue

    async def _async_worker(self):
        while not self._stop.is_set():
            try:
                item = self.input_queue.get(timeout=0.5)
                logger.flow(f"PULL [{self.name}]")
                if item is SENTINEL:
                    with self._sentinel_lock:
                        self._sentinels_received += 1
                        if self._sentinels_received == len(
                            self.upstream
                        ):
                            self._send_sentinel_downstream()
                            self._stop.set()
                    continue
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

    def _send_sentinel_downstream(self):
        if self.downstream:
            self.downstream.input_queue.put(SENTINEL)

    def signal_stop(self):
        self._stop.set()

    def run_source_sync(self):
        logger.info(f"Running source '{self.name}' (sync)")
        try:
            match self.kind:
                case "sync_gen":
                    for it in self.func():
                        self._send_downstream(it)
                case "sync":
                    out = self.func()
                    if out is not None:
                        self._send_downstream(out)
                case _:
                    logger.error(
                        f"Unexpected source kind: {self.kind}"
                    )
        except Exception:
            logger.exception(f"Source '{self.name}' failed")
        finally:
            self._send_sentinel_downstream()

    async def run_source_async(self):
        logger.info(f"Running source '{self.name}' (async)")
        try:
            match self.kind:
                case "async_gen":
                    async for it in self.func():
                        self._send_downstream(it)
                case "async":
                    out = await self.func()
                    if out is not None:
                        self._send_downstream(out)
                case _:
                    logger.error(
                        f"Unexpected async source kind: {self.kind}"
                    )
        except Exception:
            logger.exception(
                f"Async source '{self.name}' failed"
            )
        finally:
            self._send_sentinel_downstream()

    def run_in_process(self):
        try:
            if self.is_webhook:
                self.start_webhook(
                    host=self.webhook_conf.get(
                        "host", "0.0.0.0"
                    ),
                    port=self.webhook_conf.get(
                        "port", 8000
                    ),
                    path=self.webhook_conf.get("path", "/"),
                    api_key=self.webhook_conf.get(
                        "api_key"
                    ),
                    model=self.webhook_conf.get("model"),
                    rate_limit_per_minute=self.webhook_conf.get(
                        "rate_limit_per_minute"
                    ),
                )
                while not self._stop.is_set():
                    time.sleep(0.1)
                if self._webhook_server is not None:
                    self._webhook_server.should_exit = True
                if self._webhook_thread:
                    self._webhook_thread.join(timeout=3)
                self._send_sentinel_downstream()
            elif not self.upstream:
                if self.kind in ["async", "async_gen"]:
                    asyncio.run(self.run_source_async())
                else:
                    self.run_source_sync()
            else:
                self.start_workers()
                while not self._stop.is_set():
                    time.sleep(0.1)
        except Exception:
            logger.exception(
                f"Process for '{self.name}' failed"
            )
        finally:
            logger.info(
                f"Process for '{self.name}' exiting"
            )

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


class Pipeline:
    """Tiny pipeline container and runner using match/case."""

    def __init__(self, a: Task, b: Task):
        a.downstream = b
        b.upstream.append(a)
        self.tasks: List[Task] = [a, b]
        self.source = a
        self.sink = b

    def __rshift__(self, other: Task):
        if not isinstance(other, Task):
            return NotImplemented
        self.sink.downstream = other
        other.upstream.append(self.sink)
        self.tasks.append(other)
        self.sink = other
        return self

    def run(self, log_level: int = logging.INFO):
        logging.basicConfig(
            level=log_level,
            format="%(levelname)-8s | %(message)s",
        )
        logger.setLevel(log_level)
        logger.info("🚀 Pipeline starting")

        procs = []
        for t in self.tasks:
            p = Process(
                target=t.run_in_process, daemon=True
            )
            p.start()
            procs.append(p)
            t._process = p

        source_tasks = [
            t for t in self.tasks if not t.upstream
        ]
        non_webhook_sources = [
            t for t in source_tasks if not t.is_webhook
        ]
        non_webhook_source_procs = [
            t._process for t in non_webhook_sources
        ]
        webhook_tasks = [
            t for t in self.tasks if t.is_webhook
        ]

        for p in non_webhook_source_procs:
            p.join()

        if webhook_tasks:
            logger.info(
                "Webhook(s) running. Ctrl+C to stop."
            )
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Shutdown requested")

        logger.info("Shutting down...")
        for t in self.tasks:
            t.signal_stop()

        for p in procs:
            p.join(timeout=5)

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


def run(p: Pipeline, log_level: int = logging.INFO):
    p.run(log_level=log_level)
