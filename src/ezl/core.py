# ezl/core.py
import asyncio
import inspect
import logging
import queue
import threading
from typing import (
    Callable,
    Any,
    List,
    Optional,
    Dict,
    Type,
)
import time
import hmac
import hashlib
from collections import defaultdict, deque
from fastapi import FastAPI, Request
from fastapi.openapi.utils import get_openapi
from fastapi.responses import (
    JSONResponse,
    PlainTextResponse,
)
import uvicorn
from pydantic import (
    BaseModel,
    ValidationError,
)

# Small compatibility: FastAPI uses Starlette under the hood for Request/Response types,
# so our previous logic works well with FastAPI's Request.

FLOW_LEVEL = 25
logging.addLevelName(FLOW_LEVEL, "FLOW")


def _flow(self, message, *args, **kwargs):
    """Custom logger method for flow events"""
    if self.isEnabledFor(FLOW_LEVEL):
        self._log(FLOW_LEVEL, message, args, **kwargs)


logging.Logger.flow = _flow

# Module logger
logger = logging.getLogger(__name__)


class Task:
    """A pipeline task that processes data items (sync or async)"""

    def __init__(
        self, func: Callable, buffer: int, workers: int
    ):
        self.func = func
        self.is_async = inspect.iscoroutinefunction(
            func
        ) or inspect.isasyncgenfunction(func)
        self.is_generator = inspect.isgeneratorfunction(
            func
        ) or inspect.isasyncgenfunction(func)
        self.buffer = buffer
        self.workers = workers
        self.name = func.__name__
        self.upstream: List["Task"] = []
        self.downstream: Optional["Task"] = None
        self.input_queue = queue.Queue(maxsize=buffer)
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []

        # webhook attributes
        self.is_webhook: bool = False
        self.webhook_conf: Dict[str, Any] = {}
        self._webhook_server: Optional[uvicorn.Server] = (
            None
        )
        self._webhook_thread: Optional[threading.Thread] = (
            None
        )

    def __rshift__(self, other: "Task") -> "Pipeline":
        """Connect tasks: task1 >> task2"""
        if not isinstance(other, Task):
            return NotImplemented
        return Pipeline(self, other)

    def start_workers(self):
        """Start worker threads (only for non-source tasks)"""
        if not self.upstream:
            logger.debug(
                f"No workers needed for source '{self.name}'"
            )
            return

        logger.info(
            f"Starting {self.workers} workers for '{self.name}'"
        )
        for i in range(self.workers):
            thread = threading.Thread(
                target=self._worker_loop,
                name=f"{self.name}-{i}",
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)

    def _worker_loop(self):
        """Dispatch to sync or async worker loop"""
        logger.debug(
            f"Worker {threading.current_thread().name} started"
        )
        if self.is_async:
            asyncio.run(self._async_worker_loop())
        else:
            self._sync_worker_loop()
        logger.debug(
            f"Worker {threading.current_thread().name} stopped"
        )

    def _sync_worker_loop(self):
        """Process items from queue using sync function"""
        while not self._stop_event.is_set():
            try:
                item = self.input_queue.get(timeout=0.5)
                logger.flow(f"PULL [{self.name}]")

                try:
                    result = self.func(item)

                    if self.is_generator:
                        for out_item in result:
                            logger.flow(
                                f"PROC [{self.name}]"
                            )
                            self._send_downstream(out_item)
                    elif result is not None:
                        logger.flow(f"PROC [{self.name}]")
                        self._send_downstream(result)
                except Exception as e:
                    logger.error(
                        f"Error in '{self.name}': {e}",
                        exc_info=True,
                    )
                finally:
                    self.input_queue.task_done()
            except queue.Empty:
                continue

    async def _async_worker_loop(self):
        """Process items from queue using async function"""
        while not self._stop_event.is_set():
            try:
                item = self.input_queue.get(timeout=0.5)
                logger.flow(f"PULL [{self.name}]")

                try:
                    result = self.func(item)

                    if inspect.isasyncgen(result):
                        async for out_item in result:
                            logger.flow(
                                f"PROC [{self.name}]"
                            )
                            self._send_downstream(out_item)
                    else:
                        out_item = await result
                        logger.flow(f"PROC [{self.name}]")
                        if out_item is not None:
                            self._send_downstream(out_item)
                except Exception as e:
                    logger.error(
                        f"Error in '{self.name}': {e}",
                        exc_info=True,
                    )
                finally:
                    self.input_queue.task_done()
            except queue.Empty:
                continue

    def _send_downstream(self, item: Any):
        """Route an item to the next task"""
        if self.downstream:
            logger.flow(
                f"PUT  [{self.name}] -> [{self.downstream.name}]"
            )
            self.downstream.input_queue.put(item)
        else:
            logger.debug(
                f"'{self.name}' sink processed item"
            )

    def signal_stop(self):
        """Signal worker threads to stop. Also stop webhook server (if any)"""
        self._stop_event.set()
        if (
            self.is_webhook
            and self._webhook_server is not None
        ):
            logger.debug(
                f"Stopping webhook server for '{self.name}'"
            )
            try:
                self._webhook_server.should_exit = True
            except Exception:
                logger.exception(
                    "Error while signaling webhook server to stop"
                )

    def wait_for_queue(self):
        """Wait until all queued items are processed"""
        if self.upstream:
            self.input_queue.join()

    def stop_gracefully(self):
        """Stop workers and wait for them to finish. Join webhook thread if present."""
        if not self._threads and not (
            self.is_webhook and self._webhook_thread
        ):
            return
        logger.debug(f"Stopping workers for '{self.name}'")
        self.signal_stop()
        for thread in self._threads:
            thread.join(timeout=2)

        if self.is_webhook and self._webhook_thread:
            self._webhook_thread.join(timeout=3)

    # --------------------------
    # webhook helpers (with auth, HMAC, validation, rate-limiting)
    # --------------------------
    def _make_fastapi_app(
        self,
        path: str,
        *,
        api_key: Optional[str] = None,
        hmac_secret: Optional[bytes] = None,
        model: Optional[Type[BaseModel]] = None,
        rate_limit_per_minute: Optional[int] = None,
    ):
        """
        Build a FastAPI app for this webhook endpoint, optionally creating:
        - single-item POST at `path`
        - typed `/batch` POST at `path + "/batch"` when `model` is provided
        The `/batch` route is annotated with List[model] so OpenAPI shows fields clearly.
        """
        app = FastAPI(title=f"EZL webhook: {self.name}")

        # Simple in-memory sliding window rate limiter
        window_seconds = 60
        if (
            rate_limit_per_minute is not None
            and rate_limit_per_minute > 0
        ):
            counters: Dict[str, deque] = defaultdict(deque)
            counters_lock = asyncio.Lock()
        else:
            counters = None
            counters_lock = None

        # -----------------------
        # Single-item endpoint
        # -----------------------
        @app.post(
            path, summary="Ingest single webhook item"
        )
        async def handle_single(request: Request):
            # Only POST — keep the check for safety
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

            # API key auth (optional)
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

            # Rate limiting (optional)
            if (
                counters is not None
                and counters_lock is not None
            ):
                now = time.time()
                async with counters_lock:
                    dq = counters[identity]
                    while (
                        dq and dq[0] <= now - window_seconds
                    ):
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

            # Parse JSON
            try:
                payload = await request.json()
            except Exception:
                return JSONResponse(
                    {"error": "Invalid JSON"},
                    status_code=400,
                )

            # If array -> instruct to use /batch
            if isinstance(payload, list):
                return JSONResponse(
                    {
                        "error": "Batch payloads should be POSTed to the /batch endpoint"
                    },
                    status_code=400,
                )

            # Validate single item (if model provided)
            accepted = 0
            rejected = 0
            if model is not None:
                try:
                    validated = model.model_validate(
                        payload
                    )
                    obj_to_enqueue = validated.model_dump()
                    accepted = 1
                except ValidationError as ve:
                    rejected = 1
                    logger.debug(
                        f"Validation failed for item: {ve}"
                    )
                    return JSONResponse(
                        {
                            "accepted": accepted,
                            "rejected": rejected,
                        },
                        status_code=422,
                    )
            else:
                obj_to_enqueue = payload
                accepted = 1

            # Enqueue
            if self.downstream:
                try:
                    self.downstream.input_queue.put(
                        obj_to_enqueue
                    )

                except queue.Full:
                    logger.flow(
                        f"WEBHOOK [{self.name}] queue full -> rejecting single item"
                    )
                    return JSONResponse(
                        {
                            "accepted": 0,
                            "rejected": 1,
                            "error": "Queue is full. Please retry after a few seconds.",
                        },
                        status_code=503,
                        headers={"Retry-After": 10},
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to enqueue item for '{self.name}': {e}"
                    )
                    return JSONResponse(
                        {"error": "Failed to enqueue"},
                        status_code=500,
                    )
            else:
                logger.warning(
                    f"Webhook '{self.name}' received data but no downstream is attached"
                )

            logger.flow(
                f"WEBHOOK [{self.name}] accepted={accepted} rejected={rejected}"
            )
            return JSONResponse(
                {
                    "accepted": accepted,
                    "rejected": rejected,
                },
                status_code=202,
            )

        # -----------------------
        # Batch endpoint (typed) - only add if a pydantic model is provided
        # -----------------------
        if model is not None:
            # create route path for batch (ensure no double-slash)
            batch_path = path.rstrip("/") + "/batch"

            async def handle_batch(
                items: list[model],  # type: ignore
            ):
                """
                Accepts a JSON array of items matching the provided pydantic model.
                This function body will run after FastAPI has validated & parsed items into model instances.
                """
                accepted = 0
                rejected = 0
                for inst in items:
                    try:
                        # inst is a model instance (validated)
                        obj_to_enqueue = inst.dict()
                        if self.downstream:
                            self.downstream.input_queue.put(
                                obj_to_enqueue
                            )
                            accepted += 1
                    except Exception as e:
                        logger.error(
                            f"Failed to enqueue item for '{self.name}': {e}"
                        )
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

            # Register the batch route. We give it an explicit summary so docs look nice.
            app.post(
                batch_path, summary="Ingest batch of items"
            )(handle_batch)

            def custom_openapi():
                if app.openapi_schema:
                    return app.openapi_schema
                openapi_schema = get_openapi(
                    title=app.title,
                    version="1.0.0",
                    routes=app.routes,
                )
                # ensure model schema is in components
                try:
                    model_schema = model.model_json_schema(
                        ref_template="#/components/schemas/{model}"
                    )
                except Exception:
                    model_schema = model.model_json_schema()

                comp = openapi_schema.setdefault(
                    "components", {}
                ).setdefault("schemas", {})
                comp.setdefault(
                    model.__name__, model_schema
                )

                path_item = openapi_schema.get(
                    "paths", {}
                ).get(path)
                if path_item and "post" in path_item:
                    post_op = path_item["post"]
                    post_op["requestBody"] = {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": f"#/components/schemas/{model.__name__}"
                                }
                            }
                        },
                        "required": True,
                    }

                # For batch path: requestBody -> array of model
                batch_item = openapi_schema.get(
                    "paths", {}
                ).get(batch_path)
                if batch_item and "post" in batch_item:
                    post_op = batch_item["post"]
                    post_op["requestBody"] = {
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

                app.openapi_schema = openapi_schema
                return app.openapi_schema

            app.openapi = custom_openapi
        else:
            # If no model provided, we still supply a default openapi so docs show the route exists
            def custom_openapi_no_model():
                if app.openapi_schema:
                    return app.openapi_schema
                openapi_schema = get_openapi(
                    title=app.title,
                    version="1.0.0",
                    routes=app.routes,
                )
                app.openapi_schema = openapi_schema
                return app.openapi_schema

            app.openapi = custom_openapi_no_model

        return app

    def start_webhook(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        path: str = "/",
        api_key: Optional[str] = None,
        hmac_secret: Optional[bytes] = None,
        model: Optional[Type[BaseModel]] = None,
        rate_limit_per_minute: Optional[int] = None,
    ):
        """
        Start a uvicorn server running a FastAPI app for this task.
        It will run in a background thread. Server will observe server.should_exit to stop.
        """
        if not self.is_webhook:
            raise RuntimeError(
                "start_webhook called on non-webhook task"
            )

        app = self._make_fastapi_app(
            path,
            api_key=api_key,
            hmac_secret=hmac_secret,
            model=model,
            rate_limit_per_minute=rate_limit_per_minute,
        )

        config = uvicorn.Config(
            app, host=host, port=port, log_level="info"
        )
        server = uvicorn.Server(config=config)

        def _run_server():
            logger.info(
                f"Starting webhook server for '{self.name}' at http://{host}:{port}{path}"
            )
            server.run()
            logger.info(
                f"Webhook server for '{self.name}' stopped"
            )

        thread = threading.Thread(
            target=_run_server,
            name=f"webhook-{self.name}",
            daemon=True,
        )
        thread.start()

        self._webhook_server = server
        self._webhook_thread = thread


class Pipeline:
    """A directed pipeline of connected tasks"""

    def __init__(self, task1: Task, task2: Task):
        task1.downstream = task2
        task2.upstream.append(task1)
        self.tasks: List[Task] = [task1, task2]
        self.source = task1
        self.sink = task2

    def __rshift__(self, other: Task) -> "Pipeline":
        """Extend pipeline: pipeline >> task"""
        if not isinstance(other, Task):
            return NotImplemented

        self.sink.downstream = other
        other.upstream.append(self.sink)
        self.tasks.append(other)
        self.sink = other
        return self

    def _run_source_sync(self, source: Task):
        """Execute a sync source task"""
        logger.info(f"Running source '{source.name}'...")
        try:
            result = source.func()
            count = 0

            if source.is_generator:
                for item in result:
                    source._send_downstream(item)
                    count += 1
            elif result is not None:
                source._send_downstream(result)
                count = 1

            logger.info(
                f"Source '{source.name}' completed ({count} items)"
            )
        except Exception as e:
            logger.error(
                f"Source '{source.name}' failed: {e}",
                exc_info=True,
            )

    async def _run_source_async(self, source: Task):
        """Execute an async source task"""
        logger.info(
            f"Running async source '{source.name}'..."
        )
        try:
            result = source.func()
            count = 0

            if inspect.isasyncgen(result):
                async for item in result:
                    source._send_downstream(item)
                    count += 1
            else:
                item = await result
                source._send_downstream(item)
                count = 1

            logger.info(
                f"Source '{source.name}' completed ({count} items)"
            )
        except Exception as e:
            logger.error(
                f"Source '{source.name}' failed: {e}",
                exc_info=True,
            )

    def run(self, log_level: int = logging.INFO):
        """
        Execute the pipeline

        Args:
            log_level: Logging level (use logging.DEBUG to see flow events)
        """
        logging.basicConfig(
            level=log_level,
            format="%(levelname)-8s | %(message)s",
        )
        logger.setLevel(log_level)

        logger.info("=" * 50)
        logger.info("🚀 Pipeline Starting...")

        # Start workers for all non-source tasks
        for task in self.tasks:
            task.start_workers()

        # Run source tasks
        source_tasks = [
            t for t in self.tasks if not t.upstream
        ]
        source_threads = []

        for src in source_tasks:
            # If the source is a webhook, start server instead of running the source function
            if getattr(src, "is_webhook", False):
                host = src.webhook_conf.get(
                    "host", "0.0.0.0"
                )
                port = src.webhook_conf.get("port", 8000)
                path = src.webhook_conf.get("path", "/")
                api_key = src.webhook_conf.get("api_key")
                hmac_secret = src.webhook_conf.get(
                    "hmac_secret"
                )
                model = src.webhook_conf.get("model")
                rate_limit = src.webhook_conf.get(
                    "rate_limit_per_minute"
                )
                src.start_webhook(
                    host=host,
                    port=port,
                    path=path,
                    api_key=api_key,
                    hmac_secret=hmac_secret,
                    model=model,
                    rate_limit_per_minute=rate_limit,
                )
                if src._webhook_thread:
                    source_threads.append(
                        src._webhook_thread
                    )
                continue

            if src.is_async:

                def target(s=src):
                    return asyncio.run(
                        self._run_source_async(s)
                    )
            else:

                def target(s=src):
                    return self._run_source_sync(s)

            thread = threading.Thread(
                target=target, daemon=True
            )
            thread.start()
            source_threads.append(thread)

        # Separate non-webhook threads from webhook threads
        non_webhook_threads = [
            th
            for th in source_threads
            if not (th.name.startswith("webhook-"))
        ]
        webhook_present = any(
            getattr(t, "is_webhook", False)
            for t in self.tasks
        )

        # Wait for non-webhook source threads to complete (block until they are done)
        for th in non_webhook_threads:
            if th.is_alive():
                th.join()

        if webhook_present:
            logger.info(
                "Webhook servers running. Press Ctrl+C to stop."
            )
            try:
                # Keep the main thread alive until interrupted.
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info(
                    "Shutdown requested (KeyboardInterrupt). Proceeding to shutdown webhooks..."
                )
        else:
            logger.info(
                "Sources complete (no webhooks). Draining queues..."
            )

        # Wait for all queues to empty
        for task in self.tasks:
            task.wait_for_queue()

        # Shutdown
        logger.info("Processing complete. Shutting down...")
        for task in self.tasks:
            task.signal_stop()
        for task in self.tasks:
            task.stop_gracefully()

        logger.info("=" * 50)
        logger.info("✅ Pipeline Finished.")


def task(buffer: int = 100, workers: int = 3):
    """
    Decorator to create a pipeline task (sync or async)

    Args:
        buffer: Max size of the input queue
        workers: Number of worker threads to process the queue
    """

    def decorator(func: Callable) -> Task:
        return Task(func, buffer, workers)

    return decorator


def webhook(
    path: str = "/",
    host: str = "0.0.0.0",
    port: int = 8000,
    buffer: int = 100,
    api_key: Optional[str] = None,
    hmac_secret: Optional[str] = None,
    model: Optional[Type[BaseModel]] = None,
    rate_limit_per_minute: Optional[int] = None,
):
    """
    Decorator to create a webhook source task with optional security and validation.

    Parameters:
      - path: endpoint path (e.g. "/ingest")
      - host, port: where to bind the server
      - buffer: task queue size for downstream task
      - api_key: if provided, requires header X-API-Key with this exact value
      - hmac_secret: if provided, expects header X-Signature which is hex HMAC-SHA256(raw_body, hmac_secret)
      - model: a pydantic BaseModel class used to validate incoming items
      - rate_limit_per_minute: simple per-identity (IP or API key) rate limit

    Usage:
        @webhook(path="/ingest", api_key="secret", hmac_secret=b"shh", model=MyModel, rate_limit_per_minute=60)
        def ingest(): pass
    """

    def decorator(func: Callable) -> Task:
        t = Task(
            func, buffer=buffer, workers=0
        )  # webhook source: no local workers needed
        t.is_webhook = True
        # store conf to be consumed by Pipeline.run/start_webhook
        t.webhook_conf = {
            "path": path,
            "host": host,
            "port": port,
            "api_key": api_key,
            "hmac_secret": hmac_secret,
            "model": model,
            "rate_limit_per_minute": rate_limit_per_minute,
        }
        return t

    return decorator


def run(pipeline: Pipeline, log_level: int = logging.INFO):
    """
    Convenience function to run a pipeline

    Args:
        pipeline: The Pipeline object to execute
        log_level: Logging level (logging.DEBUG shows flow events)
    """
    pipeline.run(log_level=log_level)
