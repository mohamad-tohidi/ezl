# tests/test_task_kinds.py
from ezl.core import Task
import asyncio


def sync_fn(x):
    return x


def sync_gen_fn(x):
    if isinstance(x, int):
        yield x


async def async_fn(x):
    return x


async def async_gen_fn(x):
    for i in range(x):
        yield i
        await asyncio.sleep(0)


def test_kind_detection_sync():
    t = Task(sync_fn, buffer=10, workers=1)
    assert t.kind == "sync"


def test_kind_detection_sync_gen():
    t = Task(sync_gen_fn, buffer=10, workers=1)
    assert t.kind == "sync_gen"


def test_kind_detection_async():
    t = Task(async_fn, buffer=10, workers=1)
    assert t.kind == "async"


def test_kind_detection_async_gen():
    t = Task(async_gen_fn, buffer=10, workers=1)
    assert t.kind == "async_gen"
