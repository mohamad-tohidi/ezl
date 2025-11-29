from ezl import task, run, collector
import random
import asyncio


@task(buffer=100, workers=3)
async def extract():
    """Extract: Async data fetch"""
    print("🔍 Extracting 50 documents (async)...")
    for i in range(50):
        await asyncio.sleep(0.05)
        yield {
            "id": f"doc_{i}",
            "text": f"Sample document {i}",
            "priority": random.choice(["high", "low"]),
        }


@task(buffer=100, workers=3)
def transform(item):
    """Transform: CPU-bound processing (sync)"""
    import time

    time.sleep(0.01)
    for j in range(random.randint(2, 3)):
        yield {
            "id": f"{item['id']}_chunk_{j}",
            "vector": [random.random() for _ in range(768)],
            "payload": {**item, "chunk": j},
        }


@task(buffer=0, workers=2)
async def load(item):
    """Load: Async database insertion"""
    await asyncio.sleep(0.05)  # async I/O
    # await async_db.insert(item)


# ========================================
# 2. BUILD PIPELINE
# ========================================

# NOTE: by adding the collector, it will track the data that has been transfared before
# and skip those
# how it works?
# it will save an item, from the incoming data, lets say the ["id"] of the incoming json (this can be configured by the user, on what to use.)
# then, when the user restarts the pipeline, the extractor will start extracting from the first data
# as it has no memory
# but the collector will check, to see if the id of incoming data is present in its unique `.log` file or not
# if it was present, it doesnt allow the extract function to inject the data into the queue

pipeline = (
    extract >> transform >> load >> collector("logs.log")
)

# ========================================
# 3. RUN
# ========================================

if __name__ == "__main__":
    print("🚀 ETL Pipeline Starting...")
    print("=" * 50)
    run(pipeline)  # run(pipeline, resume=True)
