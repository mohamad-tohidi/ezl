import time
from ezl.core import task, Pipeline


def test_sync_task_blocks_when_downstream_full():
    called = []

    @task(buffer=1, workers=1)
    def source():
        for i in range(3):
            yield i

    @task(buffer=1, workers=1)
    def sink(x):
        # slow sink: simulate processing delay
        time.sleep(0.2)
        called.append(x)
        return x

    # connect tasks
    source >> sink
    p = Pipeline(source, sink)

    start = time.time()
    p.run(log_level=0)
    duration = time.time() - start

    # since sink sleeps, source must have been blocked at some point
    assert duration >= 0.6  # ~3 items * 0.2s each
    assert called == [0, 1, 2]
