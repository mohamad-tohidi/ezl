import asyncio
import janus
import inspect
from typing import Callable, Any, TypeVar, List, Optional
from tqdm.asyncio import tqdm

T = TypeVar("T")


class Chan:
    """Hybrid channel with both sync/async interfaces, backed by janus.Queue."""

    def __init__(
        self,
        maxsize: int = 0,
        name: str = "Channel",
        position: int = 0,
    ):
        self._q = janus.Queue(maxsize=maxsize or 0)
        self._closed = False
        self.name = name

        # Progress bar for buffered channels
        self._progress: Optional[tqdm] = None
        if maxsize > 0:
            self._progress = tqdm(
                total=maxsize,
                desc=f"{self.name} fill:",
                unit="item",
                position=position,
                colour="blue",
                leave=True,
                dynamic_ncols=True,
            )

    # === Async Interface (used by pipeline workers) ===
    async def put(self, item: T) -> None:
        """Put item asynchronously."""
        if self._closed:
            raise ValueError(
                f"Cannot send on closed channel: {self.name}"
            )
        await self._q.async_q.put(item)
        if self._progress:
            self._progress.update(1)

    async def get(self) -> T:
        """Get item asynchronously."""
        item = await self._q.async_q.get()
        self._q.async_q.task_done()
        if self._progress:
            self._progress.update(-1)
        return item

    # === Sync Interface (for direct user access) ===
    def sync_put(self, item: T) -> None:
        """Put item synchronously."""
        if self._closed:
            raise ValueError(
                f"Cannot send on closed channel: {self.name}"
            )
        self._q.sync_q.put(item)
        if self._progress:
            self._progress.update(1)

    def sync_get(self) -> T:
        """Get item synchronously."""
        item = self._q.sync_q.get()
        self._q.sync_q.task_done()
        if self._progress:
            self._progress.update(-1)
        return item

    # === Aliases ===
    send = sync_put
    recv = sync_get
    asend = put
    arecv = get

    # === Lifecycle & Iteration ===
    def close(self) -> None:
        """Close the channel."""
        if not self._closed:
            self._closed = True
            print(f"[Chan] {self.name} closed.")

    def __aiter__(self):
        """Enable async for loops."""
        return self

    async def __anext__(self) -> T:
        """Async iterator protocol."""
        if self._closed and self._q.async_q.empty():
            raise StopAsyncIteration
        return await self.get()


def chan(
    maxsize: int = 0,
    name: str = "Channel",
    position: int = 0,
) -> Chan:
    return Chan(
        maxsize=maxsize, name=name, position=position
    )


async def call_user_func(func: Callable, *args) -> Any:
    """Execute sync/async functions transparently."""
    if inspect.iscoroutinefunction(func):
        return await func(*args)
    return await asyncio.to_thread(func, *args)


async def process_results(
    result: Any, out_ch: Optional[Chan]
) -> None:
    """Handle single values, generators, and async generators."""
    if not out_ch:
        return

    if inspect.isgenerator(result):
        # Sync generator → thread pool
        loop = asyncio.get_event_loop()

        def iterate():
            for item in result:
                asyncio.run_coroutine_threadsafe(
                    out_ch.put(item), loop
                )

        await asyncio.to_thread(iterate)
    elif inspect.isasyncgen(result):
        async for item in result:
            await out_ch.put(item)
    else:
        # Single value
        await out_ch.put(result)


async def run_worker(
    func: Callable,
    in_ch: Optional[Chan],
    out_ch: Optional[Chan],
    name: str,
    role: str,
) -> None:
    """Unified worker for all task types."""
    try:
        if role == "producer":
            result = await call_user_func(func)
            await process_results(result, out_ch)
        elif role == "consumer":
            async for item in in_ch:
                await call_user_func(func, item)
        else:  # transformer
            async for item in in_ch:
                result = await call_user_func(func, item)
                await process_results(result, out_ch)
    except Exception as e:
        print(f"[{name}] Error: {e}")
        raise


class Task:
    """ETL task with auto-detected role."""

    def __init__(
        self,
        func: Callable,
        buffer: int = 0,
        workers: int = 1,
    ):
        self.func = func
        self.buffer = buffer
        self.workers = workers
        self.name = func.__name__

        sig = inspect.signature(func)
        has_params = bool(sig.parameters)
        is_gen = inspect.isgeneratorfunction(
            func
        ) or inspect.isasyncgenfunction(func)

        if is_gen and not has_params:
            self.role = "producer"
        elif is_gen and has_params:
            self.role = "transformer"
        elif not is_gen and has_params:
            self.role = "consumer"
        else:
            raise ValueError(
                f"Task '{self.name}' must be producer, transformer, or consumer"
            )

    def __rshift__(self, other: "Task") -> "Pipeline":
        return Pipeline([self, other])

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def create_channel(self, position: int = 0) -> Chan:
        return chan(
            maxsize=self.buffer,
            name=self.name,
            position=position,
        )


class Pipeline:
    """Chain of tasks with auto-wired channels."""

    def __init__(self, tasks: List[Task]):
        self.tasks = tasks

    def __rshift__(self, task: Task) -> "Pipeline":
        return Pipeline(self.tasks + [task])

    def run(self) -> None:
        if not self.tasks:
            print("No tasks to run.")
            return

        print(
            f"\n🚀 Starting pipeline with {len(self.tasks)} tasks..."
        )

        # Create channels (with positioned progress bars)
        channels = [
            task.create_channel(position=i)
            for i, task in enumerate(self.tasks[:-1])
        ]

        # Build worker tasks
        workers = []
        for i, task in enumerate(self.tasks):
            in_ch = channels[i - 1] if i > 0 else None
            out_ch = (
                channels[i] if i < len(channels) else None
            )

            for wid in range(task.workers):
                name = (
                    f"{task.name}-{wid}"
                    if task.workers > 1
                    else task.name
                )
                workers.append(
                    run_worker(
                        task.func,
                        in_ch,
                        out_ch,
                        name,
                        task.role,
                    )
                )

        # Run and cleanup
        asyncio.run(self._execute(workers, channels))

    async def _execute(
        self,
        workers: List[asyncio.Task],
        channels: List[Chan],
    ) -> None:
        await asyncio.gather(
            *workers, return_exceptions=True
        )
        for ch in channels:
            ch.close()


def task(
    buffer: int = 0, workers: int = 1
) -> Callable[[Callable], Task]:
    def decorator(func: Callable) -> Task:
        return Task(func, buffer=buffer, workers=workers)

    return decorator


def run(main: Callable = None) -> None:
    """Main entry point with auto-detection."""
    if main is not None:
        asyncio.run(main())
        return

    import __main__

    for obj in vars(__main__).values():
        if isinstance(obj, Pipeline):
            obj.run()
            return

    raise RuntimeError(
        "No pipeline found. Either:\n"
        "  1. Call run(my_async_function)\n"
        "  2. Define tasks with @task and chain with >>"
    )
