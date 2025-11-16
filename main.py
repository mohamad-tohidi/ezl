from ezl import task, run
import asyncio
import random

# ========================================
# 1. DEFINE YOUR TASKS
# ========================================


@task(buffer=100, workers=1)
async def extract():
    """Extract: Pull data from source"""
    print("🔍 Extracting 50 documents...")
    for i in range(50):
        await asyncio.sleep(0.05)
        yield {
            "id": f"doc_{i}",
            "text": f"Sample document {i}",
            "priority": random.choice(["high", "low"]),
        }


@task(buffer=100, workers=3)
async def transform(item):
    """Transform: Process data (generates 2-3 chunks per doc)"""
    await asyncio.sleep(0.1)  # Simulate CPU work

    for j in range(random.randint(2, 3)):
        yield {
            "id": f"{item['id']}_chunk_{j}",
            "vector": [random.random() for _ in range(768)],
            "payload": {**item, "chunk": j},
        }


@task(buffer=50, workers=2)
async def load(item):
    """Load: Insert into target"""
    await asyncio.sleep(0.05)  # Simulate I/O
    # print(f"💾 Loaded: {item['id']}")


# ========================================
# 2. BUILD PIPELINE
# ========================================

pipeline = extract >> transform >> load

# ========================================
# 3. RUN
# ========================================

if __name__ == "__main__":
    print("🚀 ETL Pipeline Starting...")
    print("=" * 50)
    run()  # Auto-detects pipeline & shows progress bars
