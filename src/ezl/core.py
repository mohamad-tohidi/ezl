# ezl/__init__.py
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
    """A pipeline task that processes data items"""
    
    def __init__(self, func: Callable, buffer: int, workers: int):
        self.func = func
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
        """Process items from the input queue"""
        logger.debug(f"Worker {threading.current_thread().name} started")
        while not self._stop_event.is_set():
            try:
                item = self.input_queue.get(timeout=0.5)
                logger.flow(f"PULL [{self.name}]")
                
                try:
                    result = self.func(item)
                    logger.flow(f"PROC [{self.name}]")
                    
                    # Handle functions that return/yield multiple items
                    if hasattr(result, '__iter__') and not isinstance(result, (str, bytes, dict)):
                        for out_item in result:
                            self._send_downstream(out_item)
                    elif result is not None:
                        self._send_downstream(result)
                except Exception as e:
                    logger.error(f"Error in '{self.name}': {e}", exc_info=True)
                finally:
                    self.input_queue.task_done()
                    
            except queue.Empty:
                continue  # Just check stop event again
                
        logger.debug(f"Worker {threading.current_thread().name} stopped")

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
        if self.upstream:  # Only relevant for non-sources
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
        # Link the tasks
        task1.downstream = task2
        task2.upstream.append(task1)
        
        # Track all tasks in the pipeline
        self.tasks: List[Task] = [task1, task2]
        self.source = task1
        self.sink = task2

    def __rshift__(self, other: Task) -> 'Pipeline':
        """Extend pipeline: pipeline >> task"""
        if not isinstance(other, Task):
            return NotImplemented
        
        # Link current sink to new task
        self.sink.downstream = other
        other.upstream.append(self.sink)
        
        # Update pipeline tracking
        self.tasks.append(other)
        self.sink = other
        return self

    def _run_source(self, source: Task):
        """Execute a source task (no input queue)"""
        logger.info(f"Running source '{source.name}'...")
        try:
            result = source.func()  # Source functions take no arguments
            
            count = 0
            if hasattr(result, '__iter__') and not isinstance(result, (str, bytes, dict)):
                for item in result:
                    source._send_downstream(item)
                    count += 1
            elif result is not None:
                source._send_downstream(result)
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
        # Setup logging
        logging.basicConfig(
            level=log_level,
            format='%(levelname)-8s | %(message)s'
        )
        logger.setLevel(log_level)
        
        logger.info("=" * 50)
        logger.info("🚀 Pipeline Starting...")
        
        # 1. Start workers for all non-source tasks
        for task in self.tasks:
            task.start_workers()
        
        # 2. Run source tasks in dedicated threads
        source_tasks = [t for t in self.tasks if not t.upstream]
        source_threads = [
            threading.Thread(target=self._run_source, args=(src,), daemon=True)
            for src in source_tasks
        ]
        
        for th in source_threads:
            th.start()
            
        # 3. Wait for sources to finish generating
        for th in source_threads:
            th.join()
        
        logger.info("Sources complete. Draining queues...")
        
        # 4. Wait for all intermediate queues to empty
        for task in self.tasks:
            task.wait_for_queue()
        
        # 5. Shutdown all workers
        logger.info("Processing complete. Shutting down...")
        for task in self.tasks:
            task.signal_stop()
        for task in self.tasks:
            task.stop_gracefully()
            
        logger.info("=" * 50)
        logger.info("✅ Pipeline Finished.")


def task(buffer: int = 100, workers: int = 3):
    """
    Decorator to create a pipeline task
    
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