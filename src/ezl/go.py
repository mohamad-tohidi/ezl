import asyncio
import janus
import inspect
from typing import Callable, Any, TypeVar
from queue import Empty

from tqdm.asyncio import tqdm


T = TypeVar("T")


class Chan:
    """
    A wrapper around janus.Queue to mimic a Go Channel,
    now supporting synchronous iteration and tqdm progress tracking on puts and gets to show fill level.
    """

    def __init__(
        self, maxsize: int = 0, name: str = "Channel"
    ):
        # janus.Queue handles the concurrent access.
        self._q: janus.Queue = janus.Queue(maxsize=maxsize)
        self._is_closed: bool = False
        self.name = name
        self._progress_bar: tqdm | None = None
        self.maxsize = maxsize

        # Aliases (send/asend point to the overridden put methods)
        self.send = self.sync_put
        self.recv = self.sync_get
        self.asend = self.async_put
        self.arecv = self.async_get

    # --- New Progress Bar Methods ---

    def init_progress(self, position: int = 0) -> None:
        """Initializes the tqdm progress bar for this channel to track fill level."""
        self._progress_bar = tqdm(
            total=self.maxsize
            if self.maxsize > 0
            else None,
            desc=f"{self.name} fill:",
            unit="item",
            position=position,
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
        if self._progress_bar is not None:
            self._progress_bar.update(1)

    async def async_put(self, item: T):
        """Puts an item asynchronously and updates the progress bar."""
        if self._is_closed:
            raise ValueError(
                "Cannot send on closed channel"
            )
        await self._q.async_q.put(item)
        if self._progress_bar is not None:
            self._progress_bar.update(1)

    def sync_get(self) -> T:
        """Gets an item synchronously, updating the progress bar, and handling close."""
        while True:
            try:
                item = self._q.sync_q.get(timeout=0.1)
                if self._progress_bar is not None:
                    self._progress_bar.update(-1)
                self._q.sync_q.task_done()
                return item
            except Empty:
                if self._is_closed:
                    raise StopIteration

    async def async_get(self) -> T:
        """Gets an item asynchronously, updating the progress bar, and handling close."""
        while True:
            try:
                item = await asyncio.wait_for(
                    self._q.async_q.get(), timeout=0.1
                )
                if self._progress_bar is not None:
                    self._progress_bar.update(-1)
                self._q.async_q.task_done()
                return item
            except asyncio.TimeoutError:
                if self._is_closed:
                    raise StopAsyncIteration

    # --- Overridden Close Method to Finalize Progress Bar ---

    def close(self) -> None:
        """
        Marks the channel as closed. Progress bar remains open to allow draining remaining items.
        """
        if not self._is_closed:
            self._is_closed = True
            print(f"[Chan] {self.name} closed.")

    # --- ASYNCHRONOUS ITERATION METHODS ---
    def __aiter__(self):
        """Returns the async iterator object."""
        return self

    async def __anext__(self) -> Any:
        """Gets the next item asynchronously, raising StopAsyncIteration on close/empty."""
        return await self.async_get()

    # --- Synchronous Iteration Methods ---

    def __iter__(self) -> "Chan":
        return self

    def __next__(self) -> Any:
        """Gets the next item synchronously, raising StopIteration on close/empty."""
        return self.sync_get()

    def __str__(self):
        # We need to use .qsize() for the janus queue size
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
        # Run sync functions in a separate thread
        coro = asyncio.to_thread(func, *args, **kwargs)

    task = loop.create_task(coro)
    return task


def run(main_coroutine: Callable[..., Any]) -> None:
    """The main entry point, analogous to main() in Go."""
    try:
        asyncio.run(main_coroutine())
    except KeyboardInterrupt:
        print("\nProgram terminated.")
