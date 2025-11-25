# tests/test_worker_processing.py
from ezl.core import task, run
import logging


# source: sync_gen
@task(buffer=20, workers=0)
def number_source():
    for i in range(5):
        yield i


# processor: sync (multiply)
@task(buffer=10, workers=2)
def multiply(x):
    return x * 2


# sink: sync that collects
collected = []


@task(buffer=10, workers=1)
def collector(x):
    collected.append(x)
    # no return -> sink


def test_sync_pipeline_processes_all():
    global collected
    collected = []
    p = number_source >> multiply >> collector
    # run in test thread to let pytest continue (run blocks until done)
    run(p, log_level=logging.WARNING)
    # after run completes all items should be processed
    assert sorted(collected) == sorted(
        [i * 2 for i in range(5)]
    )
