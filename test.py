from ezl import task, run
import asyncio

processed_values = []
total_processed = 0


@task(buffer=10000, workers=1)
async def extract():
    """Extract: Generate 10,000 sequential numbers for verifiable processing"""
    print("🔍 Extracting 10,000 numbers...")
    for i in range(10000):
        await asyncio.sleep(0.001)
        yield {
            "id": f"num_{i}",
            "value": i,
        }
    print("✅ Extraction complete.")


@task(buffer=10000, workers=20)
async def transform(item):
    """Transform: Double the value (simple verifiable operation, 1:1 mapping)"""
    await asyncio.sleep(0.005)
    doubled_value = item["value"] * 2
    yield {
        "id": item["id"],
        "value": doubled_value,
        "original": item["value"],
    }


@task(buffer=5000, workers=1)
async def load(item):
    """Load: Collect transformed values for verification"""
    global total_processed
    await asyncio.sleep(0.002)
    processed_values.append(item["value"])
    total_processed += 1
    if total_processed % 1000 == 0:
        print(
            f"📦 Loaded {total_processed} items so far..."
        )


# ========================================
# 2. BUILD PIPELINE
# ========================================
pipeline = extract >> transform >> load

# ========================================
# 3. RUN
# ========================================
if __name__ == "__main__":
    print("🚀 Torture Test ETL Pipeline Starting...")
    print("=" * 60)
    print("Configuration:")
    print(
        "- Extract: 10,000 items, 1 worker, buffer=10,000"
    )
    print("- Transform: 100 workers, buffer=10,000")
    print("- Load: 50 workers, buffer=5,000")
    print("Expected results:")
    print("- Total items processed: 10,000")
    print("- Expected sum of processed values: 99,990,000")
    print("=" * 60)

    run(pipeline=pipeline)

    # Verification after pipeline completes
    actual_count = len(processed_values)
    actual_sum = sum(processed_values)
    expected_sum = sum(
        2 * i for i in range(10000)
    )  # 2 * (0 + 1 + ... + 9999) = 99,990,000

    print("\n" + "=" * 60)
    print("🔍 VERIFICATION RESULTS:")
    print(
        f"Total items processed: {actual_count} (Expected: 10,000)"
    )
    print(
        f"Sum of processed values: {actual_sum} (Expected: {expected_sum})"
    )
    if actual_count == 10000 and actual_sum == expected_sum:
        print(
            "✅ PIPELINE SUCCEEDED: All items processed correctly!"
        )
    else:
        print(
            "❌ PIPELINE FAILED: Mismatch in count or sum detected."
        )
        if actual_count != 10000:
            print(
                f"   - Missing/Duplicate items: {actual_count - 10000}"
            )
        if actual_sum != expected_sum:
            print(
                f"   - Sum mismatch: {actual_sum - expected_sum}"
            )
    print("=" * 60)
