# pipeline.py - Minimal ETL pipeline DSL built on top of your existing go.py Chan/chan/go/run

import asyncio
import inspect
from tqdm.asyncio import tqdm as tqdm

from .go import chan, Chan

# Globals for the DSL (reset on every run)
_current_sink: "Stage | None" = None
_next_position = 0


def _get_next_position() -> int:
    global _next_position
    pos = _next_position
    _next_position += 1
    return pos


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
                "Can only chain @task-decorated stages with >>"
            )

        ch = chan(
            maxsize=self.buffer,
            name=f"{self.name} → {other.name} fill:",
        )
        ch.init_progress(position=_get_next_position())

        self.output_chan = ch
        other.input_chan = ch
        other.prev = self

        global _current_sink
        _current_sink = other
        return other


async def _stage_worker(stage: Stage, worker_id: int):
    try:
        if stage.is_source:
            async for item in stage.func():
                if stage.output_chan:
                    await stage.output_chan.async_put(item)
        else:
            async for item in stage.input_chan:
                result = stage.func(item)
                if inspect.isasyncgen(result):
                    async for out_item in result:
                        if stage.output_chan:
                            await (
                                stage.output_chan.async_put(
                                    out_item
                                )
                            )
                else:  # plain async coroutine (sink-style)
                    await result
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(
            f"[ERROR] {stage.name} worker {worker_id}: {e}"
        )
        raise


def get_stages(sink: Stage) -> list[Stage]:
    stages = []
    current = sink
    while current is not None:
        stages.append(current)
        current = current.prev
    stages.reverse()
    return stages


async def _run_async(stages: list[Stage]):
    worker_tasks_per_stage = []
    for stage in stages:
        stage_tasks = [
            asyncio.create_task(_stage_worker(stage, i))
            for i in range(stage.workers)
        ]
        worker_tasks_per_stage.append(stage_tasks)

    try:
        for i, stage in enumerate(stages):
            if stage.workers > 0:
                await asyncio.gather(
                    *worker_tasks_per_stage[i]
                )
            if stage.output_chan is not None:
                stage.output_chan.close()
    finally:
        # Cancel everything on error / KeyboardInterrupt
        all_tasks = [
            t for sub in worker_tasks_per_stage for t in sub
        ]
        for t in all_tasks:
            if not t.done():
                t.cancel()


def run():
    global _current_sink, _next_position
    if _current_sink is None:
        raise RuntimeError(
            "No pipeline built. Build one with >> first (e.g. extract >> transform >> load)"
        )
    stages = get_stages(_current_sink)
    _next_position = (
        0  # reset progress bar positions for next run
    )
    asyncio.run(_run_async(stages))
    _current_sink = None


def task(
    *,
    buffer: int = 0,
    workers: int = 1,
    name: str | None = None,
):
    """Decorator that turns an async generator / async function into a pipeline stage."""

    def decorator(func):
        s = Stage(
            func, buffer=buffer, workers=workers, name=name
        )
        return s

    return decorator
