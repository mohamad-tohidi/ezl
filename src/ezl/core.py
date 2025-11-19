import asyncio
import inspect
import logging
import threading
import queue  # <--- Added
from typing import Any, AsyncIterator  # <--- Added

from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
)
from rich.live import Live
from rich.panel import Panel
from rich.logging import RichHandler

# ------------------------------------------------------------------
# FIXED CHANNEL IMPLEMENTATION
# ------------------------------------------------------------------
_SENTINEL = object()


class Chan:
    def __init__(
        self, maxsize: int = 0, name: str = "Chan"
    ):
        self.q: queue.Queue = queue.Queue(maxsize=maxsize)
        self.name = name
        self.task_id = None
        self._closed = False

    async def async_put(self, item: Any):
        if self._closed:
            return  # or raise, but usually we just want to stop
        await asyncio.to_thread(self.q.put, item)

    def close(self):
        if not self._closed:
            self._closed = True
            self.q.put(_SENTINEL)

    def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self):
        item = await asyncio.to_thread(self.q.get)
        if item is _SENTINEL:
            self.q.put(
                _SENTINEL
            )  # Re-queue for other consumers
            raise StopAsyncIteration
        return item


def chan(maxsize=0, name=""):
    return Chan(maxsize=maxsize, name=name)


# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
logger = logging.getLogger("ezl")
logger.handlers.clear()
logger.setLevel(logging.INFO)
logger.addHandler(
    RichHandler(
        show_time=True,
        show_path=False,
        markup=True,
        rich_tracebacks=True,
    )
)

# ------------------------------------------------------------------
# Stage & task
# ------------------------------------------------------------------
_current_sink: "Stage | None" = None


class Stage:
    def __init__(
        self,
        func,
        *,
        buffer: int,
        workers: int = 1,
        name: str | None = None,
    ):
        self.func = func
        self.buffer = buffer
        self.workers = workers
        self.name = name or func.__name__
        self.input_chan: "Chan | None" = None
        self.output_chan: "Chan | None" = None
        self.prev: "Stage | None" = None
        self.is_source = (
            len(inspect.signature(func).parameters) == 0
        )

    def __rshift__(self, other: "Stage") -> "Stage":
        if not isinstance(other, Stage):
            raise TypeError(
                "Only @task stages can be chained with >>"
            )

        # Uses the thread-safe Queue wrapper now
        ch = chan(
            maxsize=self.buffer,
            name=f"{self.name}→{other.name}",
        )
        self.output_chan = ch
        other.input_chan = ch
        other.prev = self
        global _current_sink
        _current_sink = other
        return other


# ------------------------------------------------------------------
# Worker Logic
# ------------------------------------------------------------------
async def _stage_worker(
    stage: Stage,
    worker_id: int,
    *,
    progress: Progress,
    overall_task: int,
    sink_stage: Stage,
):
    tag = f"[{stage.name} w{worker_id:02d}]"
    try:
        # logger.info(f"{tag} Starting") # Reduced noise

        if stage.is_source:
            async for item in stage.func():
                if stage.output_chan:
                    await stage.output_chan.async_put(item)
                    if hasattr(
                        stage.output_chan, "task_id"
                    ):
                        progress.update(
                            stage.output_chan.task_id,
                            advance=1,
                        )

                    if stage is sink_stage:
                        progress.update(
                            overall_task, advance=1
                        )
        else:
            # This now works because __anext__ uses to_thread(q.get)
            async for item in stage.input_chan:
                # drain buffer bar logic
                if hasattr(stage.input_chan, "task_id"):
                    progress.update(
                        stage.input_chan.task_id, advance=-1
                    )

                # Process
                result = stage.func(item)

                # Handle Async Generators vs Standard Coroutines
                if inspect.isasyncgen(result):
                    async for out_item in result:
                        if stage.output_chan:
                            await (
                                stage.output_chan.async_put(
                                    out_item
                                )
                            )
                            if hasattr(
                                stage.output_chan, "task_id"
                            ):
                                progress.update(
                                    stage.output_chan.task_id,
                                    advance=1,
                                )
                elif inspect.isawaitable(result):
                    out_item = await result
                    # If a normal task returns a value, pass it on?
                    # Your original logic didn't pass on simple return values,
                    # but usually a pipeline passes data.
                    # Assuming 'result' is just the side effect or data:
                    if (
                        stage.output_chan
                        and out_item is not None
                    ):
                        await stage.output_chan.async_put(
                            out_item
                        )
                        if hasattr(
                            stage.output_chan, "task_id"
                        ):
                            progress.update(
                                stage.output_chan.task_id,
                                advance=1,
                            )

                # Update Sink Counter
                if stage is sink_stage:
                    progress.update(overall_task, advance=1)

        logger.info(f"{tag} Completed")

    except asyncio.CancelledError:
        return
    except Exception:
        logger.exception(f"{tag} FAILED")
        raise


def _thread_entrypoint(
    stage, worker_id, progress, overall_task, sink_stage
):
    try:
        asyncio.run(
            _stage_worker(
                stage,
                worker_id,
                progress=progress,
                overall_task=overall_task,
                sink_stage=sink_stage,
            )
        )
    except Exception as e:
        logger.error(
            f"Thread for {stage.name} crashed: {e}"
        )


def _stage_manager(
    stage, progress, overall_task, sink_stage
):
    threads = []
    for i in range(stage.workers):
        t = threading.Thread(
            target=_thread_entrypoint,
            args=(
                stage,
                i,
                progress,
                overall_task,
                sink_stage,
            ),
            name=f"{stage.name}-w{i}",
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # Close output channel to signal next stage
    if ch := stage.output_chan:
        ch.close()  # Puts Sentinel
        logger.info(f"Closed channel │ {ch.name}")


# ------------------------------------------------------------------
# Run Orchestrator
# ------------------------------------------------------------------
def get_stages(sink: Stage) -> list[Stage]:
    stages = []
    cur = sink
    while cur:
        stages.append(cur)
        cur = cur.prev
    stages.reverse()
    return stages


def run(setup_callback=None, teardown_callback=None):
    global _current_sink
    if _current_sink is None:
        raise RuntimeError("No pipeline defined")
    stages = get_stages(_current_sink)

    # Setup Progress
    progress = Progress(
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        expand=True,
    )

    overall_task = progress.add_task(
        "[bold green]Total processed", total=None
    )

    for i, stage in enumerate(stages[:-1]):
        ch = stage.output_chan
        next_stage = stages[i + 1]
        desc = f"{stage.name} → {next_stage.name}"
        task_id = progress.add_task(
            desc, total=ch.q.maxsize or None, completed=0
        )
        ch.task_id = task_id

    if setup_callback:
        asyncio.run(setup_callback())

    try:
        with Live(
            Panel(
                progress,
                title="🚀 EZL Pipeline",
                border_style="blue",
            ),
            refresh_per_second=10,
        ):
            stage_manager_threads = []
            for stage in stages:
                t = threading.Thread(
                    target=_stage_manager,
                    args=(
                        stage,
                        progress,
                        overall_task,
                        stages[-1],
                    ),
                    name=f"Manager-{stage.name}",
                )
                t.start()
                stage_manager_threads.append(t)

            for t in stage_manager_threads:
                t.join()
    except KeyboardInterrupt:
        pass
    finally:
        if teardown_callback:
            asyncio.run(teardown_callback())
        _current_sink = None


def task(
    buffer: int = 0,
    workers: int = 1,
    name: str | None = None,
):
    def decorator(func):
        return Stage(
            func, buffer=buffer, workers=workers, name=name
        )

    return decorator
