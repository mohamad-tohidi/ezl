
# **ezl — Extremely Small Pipeline Library**

Easy ETL -> EZL


`ezl` is a tiny, opinionated pipeline engine built around **Tasks**, **queues**, and optional **webhook ingestion**.
It focuses on **simplicity**, **predictable backpressure**, and **minimal Developer Experience**.

---

## **Features at a Glance**

* **Composable pipeline syntax:**

  ```python
  p = source >> process >> sink
  run(p)
  ```

* **Four task kinds detected automatically:**

  * `sync`
  * `sync_gen` (generator → many outputs per input)
  * `async`
  * `async_gen` (async generator)

* **Deterministic backpressure** using blocking `queue.put()` in worker threads.

* **Webhook ingestion** with:

  * FastAPI auto-generated single + batch endpoints
  * Optional Pydantic validation
  * Built-in rate limiting
  * Automatic OpenAPI schema injection
  * API key support

* **Minimal, traceable logging**, including a custom `FLOW` level (25) to visualize pull/put behavior.

---

## **When to Use `ezl`**

### ✅ **Good For**

* **Small to medium ETL pipelines**

  * text processing, event enrichment, parsing, transformations
* **Offline, sequential or multi-stage workflows**

  * batch jobs, file-based processing
* **Lightweight streaming**

  * “pipe items through tasks” with predictable backpressure
* **Webhook → background processing** pipelines

  * ingest data from external systems without building full infra
* **Rapid prototyping**

  * tiny codebase, easy to understand
* **Hobby-scale or single-node data processing**

---

### ❌ **Not Good For**

* **Distributed or large-scale data processing**

  * no clustering, no partitioning, no distributed queues
* **High-availability webhook ingestion**

  * in-memory queue only; no persistence or replication
* **Long-running or resource-hungry async workloads**

  * threads + asyncio coexist but not optimized for high concurrency
* **Multi-sink routing or DAGs**

  * the pipeline is strictly linear: one downstream per task
* **High throughput message streaming**

  * no batching at the worker level, no Kafka/RabbitMQ integration

---

## **Concept Overview**

### **Task**

A unit of computation.
Created with:

```python
@task(buffer=100, workers=3)
def my_task(x):
    return x * 2
```

A task can be:

* a **source** (no upstream)
* a **worker** (pulls from upstream queue)
* a **sink** (no downstream)

### **Pipeline**

Linear chain of tasks:

```python
p = source >> step1 >> step2 >> sink
run(p)
```

### **Webhook Source**

Turns a task into a FastAPI server:

```python
@webhook(path="/ingest", model=MyModel)
def incoming():
    pass
```

Incoming HTTP POST data is validated and queued into downstream tasks.

---

## **Quick Example**

```python
from ezl import task, run

@task(workers=0)
def source():
    for i in range(5):
        yield i

@task(workers=2)
def double(x):
    return x * 2

@task(workers=1)
def printer(x):
    print("out:", x)

pipeline = source >> double >> printer
run(pipeline)
```

---

## **Strengths**

| Strength                 | Description                                                           |
| ------------------------ | --------------------------------------------------------------------- |
| **Simplicity**           | Very small codebase, easy to extend or customize.                     |
| **Predictable behavior** | Blocking backpressure ensures slow sinks naturally throttle upstream. |
| **Flexible task types**  | Sync, async, generators, async generators.                            |
| **Webhook integration**  | FastAPI-based ingestion with validation and OpenAPI.                  |
| **Debuggability**        | FLOW-level logs show item movement through the pipeline.              |

---

## **Weaknesses / Limitations**

| Limitation                | Impact                                                |
| ------------------------- | ----------------------------------------------------- |
| **In-memory queues only** | No durability; crashes lose items.                    |
| **Single downstream**     | DAGs, branching, joining not supported.               |
| **Threads for workers**   | Not ideal for CPU-heavy workloads or massive scaling. |
| **Single-process only**   | No clustering or distributed pipelines.               |
| **Webhook backpressure**  | Full queues cause immediate 503 responses.            |

---

## **Installation**


```bash
pip install py-ezl 

# or if you are smart
uv add py-ezl
```

