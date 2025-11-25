# tests/test_error_handling.py
from ezl.core import task
import logging


# producer source
@task(buffer=10, workers=0)
def good_source():
    for i in range(3):
        yield i


# processor that raises on a certain input
raised = {"count": 0}


@task(buffer=10, workers=1)
def flaky(x):
    if x == 1:
        raised["count"] += 1
        raise RuntimeError("boom")
    return x + 10


collected = []


@task(buffer=10, workers=1)
def sink(x):
    collected.append(x)


def test_workers_continue_after_exception():
    global collected, raised
    collected = []
    raised = {"count": 0}
    p = good_source >> flaky >> sink
    # run pipeline
    p.run(log_level=logging.WARNING)
    # despite one raising, other items should be processed
    # items processed: 0 -> 10, 1 -> raises, 2 -> 12
    assert 10 in collected
    assert 12 in collected
    assert raised["count"] == 1
