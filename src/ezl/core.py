# pipeline.py - Enhanced with structured, tagged logging for full observability

import asyncio
import inspect
import logging
from .go import chan, Chan

# ------------------------------------------------------------------
# Logging setup - clean, tagged, colored-optional, no duplicate handlers
# ------------------------------------------------------------------
logger = logging.getLogger("ezl")
if not logger.handlers:
    logger.setLevel(
        logging.INFO
    )  # Change to DEBUG globally if you want per-item logs later

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# ------------------------------------------------------------------
# Globals for the DSL
# ------------------------------------------------------------------
_current_sink: "Stage | None" = None
_next_position = 0


def _get_next_position() -> int:
    global _next_position
    pos = _next_position
    _next_position += 1
    return pos


# ------------------------------------------------------------------
# Stage & task decorator
# ------------------------------------------------------------------
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
                "Only @task-decorated functions can be chained with >>"
            )

        ch = chan(
            maxsize=self.buffer,
            name=f"{self.name} → {other.name}",
        )
        ch.init_progress(position=_get_next_position())

        self.output_chan = ch
        other.input_chan = ch
        other.prev = self

        global _current_sink
        _current_sink = other
        return other


# ------------------------------------------------------------------
# Worker - now with detailed per-worker logging and counters
# ------------------------------------------------------------------
async def _stage_worker(stage: Stage, worker_id: int):
    tag = f"[{stage.name} w{worker_id:02d}]"
    received = produced = 0

    try:
        logger.info(f"{tag} Starting")

        if stage.is_source:
            async for item in stage.func():
                produced += 1
                if stage.output_chan:
                    await stage.output_chan.async_put(item)
        else:
            async for item in stage.input_chan:
                received += 1
                result = stage.func(item)

                if inspect.isasyncgen(result):
                    async for out_item in result:
                        produced += 1
                        if stage.output_chan:
                            await (
                                stage.output_chan.async_put(
                                    out_item
                                )
                            )
                else:  # sink
                    await result

        logger.info(
            f"{tag} Completed │ Received: {received:,} │ Produced: {produced:,}"
        )

    except asyncio.CancelledError:
        logger.debug(
            f"{tag} Cancelled (normal during shutdown)"
        )
        return
    except Exception:
        logger.exception(
            f"{tag} FAILED │ Received: {received:,} │ Produced: {produced:,}"
        )
        raise


# ------------------------------------------------------------------
# Pipeline construction helpers
# ------------------------------------------------------------------
def get_stages(sink: Stage) -> list[Stage]:
    stages = []
    current = sink
    while current is not None:
        stages.append(current)
        current = current.prev
    stages.reverse()
    return stages


# ------------------------------------------------------------------
# Run - all stages run concurrently, orderly shutdown with channel closing
# ------------------------------------------------------------------
async def _run_async(stages: list[Stage]):
    logger.info("═" * 60)
    logger.info(
        f"Pipeline STARTING │ Stages: {' → '.join(s.name for s in stages)}"
    )
    logger.info("═" * 60)

    # Create all worker tasks upfront (they start running immediately)
    worker_tasks_per_stage: list[list[asyncio.Task]] = []
    for stage in stages:
        stage_tasks = [
            asyncio.create_task(_stage_worker(stage, i))
            for i in range(stage.workers)
        ]
        worker_tasks_per_stage.append(stage_tasks)

    try:
        # Wait for each stage in order and close its output channel
        for i, stage in enumerate(stages):
            if stage.workers > 0:
                await asyncio.gather(
                    *worker_tasks_per_stage[i],
                    return_exceptions=False,
                )

            if stage.output_chan:
                stage.output_chan.close()
                logger.info(
                    f"Closed channel │ {stage.output_chan.name}"
                )

        logger.info("Pipeline completed successfully")

    finally:
        # Safety net - cancel everything still running
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
            "No pipeline defined. Use extract >> transform >> load first."
        )

    stages = get_stages(_current_sink)
    _next_position = 0
    try:
        asyncio.run(_run_async(stages))
    finally:
        _current_sink = None


# ------------------------------------------------------------------
# Decorator
# ------------------------------------------------------------------
def task(
    *,
    buffer: int = 0,
    workers: int = 1,
    name: str | None = None,
):
    def decorator(func):
        return Stage(
            func, buffer=buffer, workers=workers, name=name
        )

    return decorator
