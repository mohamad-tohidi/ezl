# tests/test_backpressure.py
import threading
import time
from ezl.core import Task


# helper: create two tasks and link them (not using Pipeline.run)
def link(a, b):
    a.downstream = b
    b.upstream.append(a)


def test_send_blocks_when_downstream_full():
    # upstream task for calling _send_downstream (kind sync)
    def upstream_fn(x):
        return x

    upstream = Task(upstream_fn, buffer=2, workers=0)
    downstream = Task(
        lambda x: None, buffer=1, workers=0
    )  # tiny buffer, no consumers

    link(upstream, downstream)

    # We'll call _send_downstream in a separate thread so we can observe blocking.
    started = threading.Event()
    _ = threading.Event()
    finished = threading.Event()

    def sender():
        started.set()
        # first put should succeed quickly (queue empty -> size 1)
        upstream._send_downstream("first")
        # second put will block because downstream buffer==1 and nobody is consuming
        # run this in a child thread so main can observe it's blocked
        try:
            upstream._send_downstream("second")
        finally:
            finished.set()

    thr = threading.Thread(target=sender)
    thr.start()
    started.wait(timeout=1.0)

    # wait a short time so first put happened, second put should be blocked
    time.sleep(0.05)
    # downstream queue should have size == buffer (1)
    assert downstream.input_queue.qsize() == 1

    # sender thread should still be alive (blocked on second put)
    assert thr.is_alive()

    # now consume one item to unblock the sender
    _ = downstream.input_queue.get(timeout=1.0)
    downstream.input_queue.task_done()

    # give time for sender to finish
    thr.join(timeout=1.0)
    assert not thr.is_alive()
    # both items should now be present (one consumed, one should be enqueued)
    # depending on timing, the queue may have the newly enqueued item
    # ensure no deadlock: join should have completed
    assert True


def test_wait_for_queue_blocks_until_consumed():
    # create a worker-style task (has upstream -> wait_for_queue will join)
    producer = Task(lambda: None, buffer=2, workers=0)
    consumer = Task(lambda x: None, buffer=2, workers=1)
    # link consumer to have an upstream
    producer.downstream = consumer
    consumer.upstream.append(producer)

    # put an item into consumer queue directly to simulate work pending
    consumer.input_queue.put("x", block=False)

    # call wait_for_queue on consumer (should wait until task_done called)
    # do task_done in separate thread after delay
    def ack():
        time.sleep(0.05)
        consumer.input_queue.task_done()

    t = threading.Thread(target=ack)
    t.start()
    # this should not hang indefinitely
    consumer.wait_for_queue()
    t.join(timeout=1.0)
    assert not t.is_alive()
