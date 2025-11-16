import asyncio
import janus
import inspect
from typing import Callable, Any, TypeVar, List
from queue import Empty

from tqdm.asyncio import tqdm

T = TypeVar("T")


class Chan:
    """A wrapper around janus.Queue to mimic a Go Channel."""

    def __init__(
        self, maxsize: int = 0, name: str = "Channel"
    ):
        self._q: janus.Queue = janus.Queue(maxsize=maxsize)
        self._is_closed: bool = False
        self.name = name
        self._progress_bar: tqdm | None = None
        self.maxsize = maxsize

        # Aliases
        self.send = self.sync_put
        self.recv = self.sync_get
        self.asend = self.async_put
        self.arecv = self.async_get

    def init_buffers_tqdm(
        self, position: int = 0, colour: str = "blue"
    ) -> None:
        """Initializes the tqdm progress bar for this channel."""
        self._progress_bar = tqdm(
            total=self.maxsize
            if self.maxsize > 0
            else None,
            desc=f"{self.name} fill:",
            unit="item",
            position=position,
            colour=colour,
            leave=True,
            dynamic_ncols=True,
        )

    def sync_put(self, item: T):
        """Puts an item synchronously and updates the progress bar."""
        if self._is_closed:
            raise ValueError(
                "Cannot send on closed channel"
            )
        self._q.sync_q.put(item)
        if self._progress_bar:
            self._progress_bar.update(1)

    async def async_put(self, item: T):
        """Puts an item asynchronously and updates the progress bar."""
        if self._is_closed:
            raise ValueError(
                "Cannot send on closed channel"
            )
        await self._q.async_q.put(item)
        if self._progress_bar:
            self._progress_bar.update(1)

    def sync_get(self) -> T:
        """Gets an item synchronously, updating the progress bar."""
        while True:
            try:
                item = self._q.sync_q.get(timeout=0.1)
                if self._progress_bar:
                    self._progress_bar.update(-1)
                self._q.sync_q.task_done()
                return item
            except Empty:
                if self._is_closed:
                    raise StopIteration

    async def async_get(self) -> T:
        """Gets an item asynchronously, updating the progress bar."""
        while True:
            try:
                item = await asyncio.wait_for(
                    self._q.async_q.get(), timeout=0.1
                )
                if self._progress_bar:
                    self._progress_bar.update(-1)
                self._q.async_q.task_done()
                return item
            except asyncio.TimeoutError:
                if self._is_closed:
                    raise StopAsyncIteration

    def close(self) -> None:
        """Marks the channel as closed."""
        if not self._is_closed:
            self._is_closed = True
            print(f"[Chan] {self.name} closed.")

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        return await self.async_get()

    def __iter__(self) -> "Chan":
        return self

    def __next__(self) -> Any:
        return self.sync_get()

    def __str__(self):
        return f"<Chan size={self._q.qsize()} closed={self._is_closed}>"


def chan(maxsize: int = 0, name: str = "Channel") -> Chan:
    """Creates a new channel."""
    return Chan(maxsize, name)


def go(
    func: Callable[..., Any], *args: Any, **kwargs: Any
) -> asyncio.Task:
    """Fires and forgets a function, handling both sync and async callable types."""
    loop = asyncio.get_event_loop()

    if inspect.iscoroutinefunction(func):
        coro = func(*args, **kwargs)
    else:
        coro = asyncio.to_thread(func, *args, **kwargs)

    return loop.create_task(coro)


def run(main_coroutine: Callable[..., Any] = None) -> None:
    """
    The main entry point, analogous to main() in Go.

    Two usage patterns:
    1. run(async_main_function) - Run a custom async function
    2. run() - Auto-detect and run a Pipeline
    """
    if main_coroutine is not None:
        try:
            asyncio.run(main_coroutine())
        except KeyboardInterrupt:
            print("\nProgram terminated.")
    else:
        # Auto-detect pipeline
        import __main__

        pipeline_obj = None

        # FIX: Use .items() to get both name and object
        for name, obj in vars(__main__).items():
            if isinstance(obj, Pipeline):
                pipeline_obj = obj
                print(f"Found pipeline: {name}")
                break

        if pipeline_obj:
            pipeline_obj.run()
        else:
            raise RuntimeError(
                "No main coroutine provided and no Pipeline found.\n"
                "Either:\n"
                "  1. Call run(my_async_function)\n"
                "  2. Define tasks with @task and chain with >>, then call run()"
            )


# ========================================
# NEW @task & PIPELINE API (MINIMAL)
# ========================================


class Task:
    """
    Wraps a user function as an ETL task.

    Role is auto-detected from function signature:
    - Producer: no parameters, yields values
    - Transformer: has parameter, yields values
    - Consumer: has parameter, doesn't yield
    """

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

        # Analyze signature
        sig = inspect.signature(func)
        self.has_params = len(sig.parameters) > 0
        self.is_generator = inspect.isgeneratorfunction(
            func
        ) or inspect.isasyncgenfunction(func)

        # Determine role
        if self.is_generator and not self.has_params:
            self.role = "producer"
        elif self.is_generator and self.has_params:
            self.role = "transformer"
        elif not self.is_generator and self.has_params:
            self.role = "consumer"
        else:
            raise ValueError(
                f"Task '{self.name}' must be one of:\n"
                f"  1. Producer: no parameters + yields (e.g., def extract(): yield item)\n"
                f"  2. Transformer: parameter + yields (e.g., def transform(item): yield item)\n"
                f"  3. Consumer: parameter + no yield (e.g., def load(item): print(item))"
            )

    def __rshift__(self, other: "Task") -> "Pipeline":
        """Enable syntax: task1 >> task2 >> task3"""
        return Pipeline([self, other])

    def __call__(self, *args, **kwargs):
        """Allow direct function calls for testing."""
        return self.func(*args, **kwargs)

    def create_channel(self) -> Chan:
        return chan(maxsize=self.buffer, name=self.name)


class Pipeline:
    """A chain of Tasks with auto-wired channels."""

    def __init__(self, tasks: List[Task]):
        self.tasks = tasks

    def __rshift__(self, task: Task) -> "Pipeline":
        """Enable chaining: pipeline >> last_task"""
        return Pipeline(self.tasks + [task])

    def run(self):
        """Execute the pipeline with all workers and channels."""
        if not self.tasks:
            print("No tasks to run.")
            return

        print(
            f"\n🚀 Starting pipeline with {len(self.tasks)} tasks..."
        )

        # Create channels between tasks
        channels = []
        for i, task in enumerate(self.tasks):
            if i < len(self.tasks) - 1:
                channels.append(task.create_channel())
            else:
                channels.append(chan(name="done"))

        for i, ch in enumerate(channels[:-1]):
            ch.init_buffers_tqdm(position=i)

        # Launch workers (rest of the code remains same)
        for i, task in enumerate(self.tasks):
            in_ch = channels[i - 1] if i > 0 else None
            out_ch = (
                channels[i]
                if task.role != "consumer"
                else None
            )

            for worker_id in range(task.workers):
                worker_name = (
                    f"{task.name}-{worker_id}"
                    if task.workers > 1
                    else task.name
                )

                if task.role == "producer":
                    go(
                        producer_worker,
                        task.func,
                        out_ch,
                        worker_name,
                    )
                elif task.role == "transformer":
                    go(
                        transformer_worker,
                        task.func,
                        in_ch,
                        out_ch,
                        worker_name,
                    )
                else:  # consumer
                    go(
                        consumer_worker,
                        task.func,
                        in_ch,
                        channels[-1],
                        worker_name,
                    )

        # Wait for completion
        async def wait_done():
            done_ch = channels[-1]
            async for _ in done_ch:
                print(
                    "\n✅ Pipeline finished successfully."
                )
                break

        run(wait_done)


def task(buffer: int = 0, workers: int = 1):
    """
    Decorator to mark a function as an ETL task.

    Args:
        buffer: Channel buffer size (maxsize) for this task's output
        workers: Number of parallel workers to spawn
    """

    def decorator(func: Callable) -> Task:
        return Task(func, buffer=buffer, workers=workers)

    return decorator


# ========================================
# WORKER WRAPPERS (MINIMAL - NO EXCEPTION HANDLING)
# ========================================


async def producer_worker(func, out_ch: Chan, name):
    """Worker for producer tasks. User code is unwrapped."""
    result = func()

    if inspect.isasyncgen(result):
        async for item in result:
            await out_ch.async_put(item)
    elif inspect.isgenerator(result):
        for item in result:
            await out_ch.async_put(item)
    else:
        raise ValueError(f"{name} must return a generator")

    out_ch.close()


async def transformer_worker(
    func, in_ch: Chan, out_ch: Chan, name
):
    """Worker for transformer tasks. User code is unwrapped."""
    async for item in in_ch:
        result = func(
            item
        )  # User handles their own exceptions

        if inspect.isasyncgen(result):
            async for out_item in result:
                await out_ch.async_put(out_item)
        elif inspect.isgenerator(result):
            for out_item in result:
                await out_ch.async_put(out_item)
        else:
            # Single return value -> treat as single yield
            await out_ch.async_put(result)

    out_ch.close()


async def consumer_worker(func, in_ch, done_ch, name):
    """Worker for consumer tasks. User code is unwrapped."""
    async for item in in_ch:
        result = func(
            item
        )  # User handles their own exceptions
        if inspect.isawaitable(result):
            await result

    await done_ch.asend(None)  # Signal completion
