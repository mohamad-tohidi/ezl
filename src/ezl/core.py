# ezl/__init__.py
import asyncio
import inspect
import logging
import queue
import threading
from typing import Callable, Any, List, Optional

# Setup custom log level for pipeline flow events
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
    
    def __init__(self, func: Callable, buffer: int, workers: int):
        self.func = func
        self.is_async = inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)
        self.is_generator = inspect.isgeneratorfunction(func) or inspect.isasyncgenfunction(func)
        self.buffer = buffer
        self.workers = workers
        self.name = func.__name__
        self.upstream: List['Task'] = []
        self.downstream: Optional['Task'] = None
        self.input_queue = queue.Queue(maxsize=buffer)
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []

    def __rshift__(self, other: 'Task') -> 'Pipeline':
        """Connect tasks: task1 >> task2"""
        if not isinstance(other, Task):
            return NotImplemented
        return Pipeline(self, other)

    def start_workers(self):
        """Start worker threads (only for non-source tasks)"""
        if not self.upstream:
            logger.debug(f"No workers needed for source '{self.name}'")
            return
        
        logger.info(f"Starting {self.workers} workers for '{self.name}'")
        for i in range(self.workers):
            thread = threading.Thread(
                target=self._worker_loop,
                name=f"{self.name}-{i}",
                daemon=True
            )
            thread.start()
            self._threads.append(thread)

    def _worker_loop(self):
        """Dispatch to sync or async worker loop"""
        logger.debug(f"Worker {threading.current_thread().name} started")
        if self.is_async:
            asyncio.run(self._async_worker_loop())
        else:
            self._sync_worker_loop()
        logger.debug(f"Worker {threading.current_thread().name} stopped")

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
                            logger.flow(f"PROC [{self.name}]")
                            self._send_downstream(out_item)
                    elif result is not None:
                        logger.flow(f"PROC [{self.name}]")
                        self._send_downstream(result)
                except Exception as e:
                    logger.error(f"Error in '{self.name}': {e}", exc_info=True)
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
                            logger.flow(f"PROC [{self.name}]")
                            self._send_downstream(out_item)
                    else:
                        out_item = await result
                        logger.flow(f"PROC [{self.name}]")
                        if out_item is not None:
                            self._send_downstream(out_item)
                except Exception as e:
                    logger.error(f"Error in '{self.name}': {e}", exc_info=True)
                finally:
                    self.input_queue.task_done()
            except queue.Empty:
                continue

    def _send_downstream(self, item: Any):
        """Route an item to the next task"""
        if self.downstream:
            logger.flow(f"PUT  [{self.name}] -> [{self.downstream.name}]")
            self.downstream.input_queue.put(item)
        else:
            logger.debug(f"'{self.name}' sink processed item")

    def signal_stop(self):
        """Signal worker threads to stop"""
        self._stop_event.set()

    def wait_for_queue(self):
        """Wait until all queued items are processed"""
        if self.upstream:
            self.input_queue.join()

    def stop_gracefully(self):
        """Stop workers and wait for them to finish"""
        if not self._threads:
            return
        logger.debug(f"Stopping workers for '{self.name}'")
        self.signal_stop()
        for thread in self._threads:
            thread.join(timeout=2)


class Pipeline:
    """A directed pipeline of connected tasks"""
    
    def __init__(self, task1: Task, task2: Task):
        task1.downstream = task2
        task2.upstream.append(task1)
        self.tasks: List[Task] = [task1, task2]
        self.source = task1
        self.sink = task2

    def __rshift__(self, other: Task) -> 'Pipeline':
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
                
            logger.info(f"Source '{source.name}' completed ({count} items)")
        except Exception as e:
            logger.error(f"Source '{source.name}' failed: {e}", exc_info=True)

    async def _run_source_async(self, source: Task):
        """Execute an async source task"""
        logger.info(f"Running async source '{source.name}'...")
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
                
            logger.info(f"Source '{source.name}' completed ({count} items)")
        except Exception as e:
            logger.error(f"Source '{source.name}' failed: {e}", exc_info=True)

    def run(self, log_level: int = logging.INFO):
        """
        Execute the pipeline
        
        Args:
            log_level: Logging level (use logging.DEBUG to see flow events)
        """
        logging.basicConfig(
            level=log_level,
            format='%(levelname)-8s | %(message)s'
        )
        logger.setLevel(log_level)
        
        logger.info("=" * 50)
        logger.info("🚀 Pipeline Starting...")
        
        # Start workers for all non-source tasks
        for task in self.tasks:
            task.start_workers()
        
        # Run source tasks
        source_tasks = [t for t in self.tasks if not t.upstream]
        source_threads = []
        
        for src in source_tasks:
            if src.is_async:
                def target(s=src):
                    return asyncio.run(self._run_source_async(s))
            else:
                def target(s=src):
                    return self._run_source_sync(s)
                
            thread = threading.Thread(target=target, daemon=True)
            thread.start()
            source_threads.append(thread)
        
        # Wait for sources to complete
        for th in source_threads:
            th.join()
        
        logger.info("Sources complete. Draining queues...")
        
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


def run(pipeline: Pipeline, log_level: int = logging.INFO):
    """
    Convenience function to run a pipeline
    
    Args:
        pipeline: The Pipeline object to execute
        log_level: Logging level (logging.DEBUG shows flow events)
    """
    pipeline.run(log_level=log_level)