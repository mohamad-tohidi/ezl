from src.ezl import task, run
import random


@task(buffer=100, workers=10)
def somthing_else():
    """Extract: Pull data from source"""
    print("🔍 Extracting 50 documents...")
    for i in range(50):
        yield {
            "id": f"doc_{i}",
            "text": f"Sample document {i}",
            "priority": random.choice(["high", "low"]),
        }


@task(buffer=100, workers=3)
def transform(item):
    """Transform: Process data (generates 2-3 chunks per doc)"""

    for j in range(random.randint(2, 3)):
        yield {
            "id": f"{item['id']}_chunk_{j}",
            "vector": [random.random() for _ in range(768)],
            "payload": {**item, "chunk": j},
        }


@task(buffer=50, workers=2)
def load(item):
    """Load: Insert into target"""
    ...

# ========================================
# 2. BUILD PIPELINE
# ========================================

pipeline = somthing_else >> transform >> load

# ========================================
# 3. RUN
# ========================================

if __name__ == "__main__":
    print("🚀 ETL Pipeline Starting...")
    print("=" * 50)
    run(pipeline)
