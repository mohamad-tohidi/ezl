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
from rich.logging import RichHandler

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
# Worker - updates Rich progress bars
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

                    # if the source is also the sink (single-stage pipeline)
                    if stage is sink_stage:
                        progress.update(
                            overall_task, advance=1
                        )
        else:
            async for item in stage.input_chan:
                # drain buffer bar
                if hasattr(stage.input_chan, "task_id"):
                    progress.update(
                        stage.input_chan.task_id, advance=-1
                    )

                # count as processed when the sink receives the item
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
                    await (
                        result
                    )  # plain async sink (e.g. load)

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

    for i, stage in enumerate(stages[:-1]):
        ch = stage.output_chan
        next_stage = stages[i + 1]
        desc = f"{stage.name} (w{stage.workers}) → {next_stage.name} buffer"
        task_id = progress.add_task(
            desc, total=ch.maxsize or None, completed=0
        )
        ch.task_id = task_id

    worker_tasks_per_stage: list[list[asyncio.Task]] = []
    for stage in stages:
        stage_tasks = [
            asyncio.create_task(
                _stage_worker(
                    stage,
                    i,
                    progress=progress,
                    overall_task=overall_task,
                    sink_stage=stages[-1],
                )
            )
            for i in range(stage.workers)
        ]
        worker_tasks_per_stage.append(stage_tasks)

    with Live(
        Panel(
            progress,
            title="🚀 Live EZL TUI",
            border_style="bright_blue",
            expand=False,
        ),
        refresh_per_second=10,
    ):
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


def run(setup_callback=None, teardown_callback=None):
    """
    Execute the defined ETL pipeline.

    This function runs the pipeline stages connected via the chaining
    operator (>>). It validates that a pipeline has been defined, logs
    the pipeline structure, and executes the stages asynchronously using
    asyncio.

    Args:
        setup_callback (callable, optional): An optional async callable
            invoked at the start of the pipeline execution, before stages
            run. Use this to initialize shared resources (e.g., database
            pools, API clients) that tasks rely on via globals or closures.
            It should be an async function (def async def setup(): ...).
            Defaults to None (no setup performed).
        teardown_callback (callable, optional): An optional async callable
            invoked at the end of the pipeline execution (in a finally block),
            after stages complete or if an error occurs. Use this to clean up
            resources initialized in setup_callback (e.g., close connections,
            release locks). It should be an async function (def async def teardown(): ...).
            Defaults to None (no teardown performed).

    Raises:
        RuntimeError: If no pipeline is defined (i.e., no stages chained).

    Returns:
        None

    Examples:
        >>> # Basic usage: No callbacks (original behavior)
        >>> from ezl import src, transform, load
        >>> @src
        >>> async def extract(): yield {"data": 1}
        >>> @transform
        >>> async def process(item): yield {"processed": item["data"]}
        >>> @load
        >>> async def sink(item): print(item)
        >>> extract >> process >> sink
        >>> run()  # Runs pipeline without setup/teardown

        >>> # With callbacks: Initialize Elasticsearch client
        >>> from elasticsearch import AsyncElasticsearch
        >>> async def setup_es():
        ...     global es
        ...     es = AsyncElasticsearch(hosts=["http://localhost:9200"])
        >>> async def teardown_es():
        ...     global es
        ...     await es.close()
        >>> # ... define stages using 'es' (e.g., in extract)
        >>> run(setup_callback=setup_es, teardown_callback=teardown_es)

        >>> # With callbacks: Database pool for asyncpg
        >>> import asyncpg
        >>> DB_CONFIG = {"host": "localhost", "database": "mydb", ...}
        >>> async def setup_db():
        ...     global db_pool
        ...     db_pool = await asyncpg.create_pool(**DB_CONFIG, min_size=1, max_size=10)
        >>> async def teardown_db():
        ...     global db_pool
        ...     if db_pool: await db_pool.close()
        >>> # ... define stages using 'db_pool' (e.g., acquire in extract)
        >>> run(setup_callback=setup_db, teardown_callback=teardown_db)

    Notes:
        - Callbacks are executed within the same event loop as the pipeline,
          ensuring proper async context for resources like AsyncElasticsearch
          or asyncpg pools.
        - Globals (e.g., 'es', 'db_pool') are commonly used for shared state,
          but consider dependency injection via closures for more robust designs.
        - Errors in setup_callback will prevent pipeline execution; errors in
          teardown_callback are logged but do not affect the pipeline result.
    """
    global _current_sink
    if _current_sink is None:
        raise RuntimeError(
            "No pipeline defined — use src >> transform >> load first"
        )
    stages = get_stages(_current_sink)
    logger.info("═" * 60)
    logger.info(
        f"Pipeline STARTING │ {' → '.join(s.name for s in stages)}"
    )
    logger.info("═" * 60)

    async def wrapped_run():
        if setup_callback:
            await setup_callback()  # e.g., init globals like es = AsyncElasticsearch(...)
        try:
            await _run_async(stages)
        finally:
            if teardown_callback:
                await teardown_callback()  # e.g., await es.close()
        _current_sink = None

    asyncio.run(wrapped_run())

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
