# pipeline.py - Rich TUI + topology line with worker fan-out highlight + orderly shutdown

import asyncio
import inspect
import logging
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
from rich.text import Text
from rich.console import Group
from rich.logging import RichHandler
from rich import box

from .go import chan, Chan

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
# Worker
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
        logger.info(f"{tag} Starting")

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
            async for item in stage.input_chan:
                if hasattr(stage.input_chan, "task_id"):
                    progress.update(
                        stage.input_chan.task_id, advance=-1
                    )

                if stage is sink_stage:
                    progress.update(overall_task, advance=1)

                result = stage.func(item)

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
                else:
                    await result

        logger.info(f"{tag} Completed")

    except asyncio.CancelledError:
        return
    except Exception:
        logger.exception(f"{tag} FAILED")
        raise


# ------------------------------------------------------------------
# Run
# ------------------------------------------------------------------
def get_stages(sink: Stage) -> list[Stage]:
    stages = []
    cur = sink
    while cur:
        stages.append(cur)
        cur = cur.prev
    stages.reverse()
    return stages


async def _run_async(stages: list[Stage]):
    if not stages:
        return

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

    sink_stage = stages[-1]

    # Buffer progress tasks
    for i, stage in enumerate(
        stages[:-1]
    ):  # all but last stage
        ch = stage.output_chan
        next_stage = stages[i + 1]
        desc = f"{stage.name} (w{stage.workers}) → {next_stage.name} buffer"
        task_id = progress.add_task(
            desc, total=ch.maxsize or None, completed=0
        )
        ch.task_id = task_id  # monkey-patch

    # Topology line (static)
    topology = Text(
        "Pipeline Topology:", style="bold bright_white"
    )
    for i, stage in enumerate(stages):
        if i > 0:
            buffer_size = stages[i - 1].buffer
            buffer_str = (
                "∞"
                if buffer_size == 0
                else str(buffer_size)
            )
            topology.append(
                f" ──► [buffer {buffer_str}] ──► ",
                style="yellow",
            )
        topology.append(
            f" [{stage.name} ×", style="bold cyan"
        )
        topology.append(
            f"{stage.workers}",
            style="bold magenta"
            if stage.workers > 1
            else "bold green",
        )
        topology.append(" workers]", style="bold cyan")

    topology_panel = Panel(
        topology,
        title=" Topology ",
        border_style="bright_cyan",
        box=box.ROUNDED,
    )

    group = Group(topology_panel, progress)

    with Live(
        Panel(
            group,
            title=" 🚀 Live Pipeline Dashboard ",
            border_style="bright_blue",
        ),
        refresh_per_second=10,
    ):
        worker_tasks_per_stage = []
        for stage in stages:
            stage_tasks = [
                asyncio.create_task(
                    _stage_worker(
                        stage,
                        i,
                        progress=progress,
                        overall_task=overall_task,
                        sink_stage=sink_stage,
                    )
                )
                for i in range(stage.workers)
            ]
            worker_tasks_per_stage.append(stage_tasks)

        try:
            for i, stage in enumerate(stages):
                await asyncio.gather(
                    *worker_tasks_per_stage[i]
                )
                if ch := stage.output_chan:
                    ch.close()
                    logger.info(
                        f"Closed channel │ {ch.name}"
                    )

            logger.info(
                "Pipeline finished — all items processed"
            )

        except Exception:
            logger.exception("Pipeline failed")
            raise
        finally:
            remaining = [
                t
                for sub in worker_tasks_per_stage
                for t in sub
                if not t.done()
            ]
            for t in remaining:
                t.cancel()


def run():
    global _current_sink
    if _current_sink is None:
        raise RuntimeError(
            "No pipeline defined — use extract >> transform >> load"
        )

    stages = get_stages(_current_sink)
    logger.info("═" * 60)
    logger.info(
        f"Pipeline STARTING │ {' → '.join(s.name for s in stages)}"
    )
    logger.info("═" * 60)

    asyncio.run(_run_async(stages))
    _current_sink = None


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
