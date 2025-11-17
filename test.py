import asyncio
import os
from src.ezl.core import task, run

TEST_DATA = [
    "hello world",
    "python pipelines",
    "async processing",
    "data flows correctly",
]


def setup_files():
    print("📁 Setting up files...")
    with open("input.txt", "w") as f:
        for line in TEST_DATA:
            f.write(line + "\n")
    if os.path.exists("output.txt"):
        os.remove("output.txt")
    print(
        f"✅ Created input.txt with {len(TEST_DATA)} lines"
    )


def verify_results():
    print("\n" + "=" * 50)
    print("🔍 VERIFICATION")
    print("=" * 50)

    with open("input.txt") as f:
        input_lines = [
            line.strip() for line in f if line.strip()
        ]

    with open("output.txt") as f:
        output_lines = [
            line.strip() for line in f if line.strip()
        ]

    if len(input_lines) != len(output_lines):
        print(
            f"❌ FAILED: Expected {len(input_lines)} lines, got {len(output_lines)}"
        )
        return False

    for i, original in enumerate(input_lines):
        expected = f"PROCESSED: {original.upper()}"
        if output_lines[i] != expected:
            print(f"❌ FAILED: Line {i} mismatch")
            print(f"   Expected: {expected}")
            print(f"   Got:      {output_lines[i]}")
            return False

    print(
        f"✅ SUCCESS: All {len(output_lines)} lines processed correctly!"
    )
    print("\n📄 Final output:")
    print("-" * 30)
    with open("output.txt") as f:
        print(f.read())
    print("-" * 30)
    return True


@task(buffer=0, workers=1)
async def extract():
    print("\n📖 Starting extraction...")
    with open("input.txt") as f:
        for idx, line in enumerate(f, 1):
            await asyncio.sleep(0.01)
            clean_line = line.strip()
            print(f"   [{idx:2d}] ← {clean_line}")
            yield clean_line


@task(
    buffer=0, workers=3
)  # Multiple workers to test the fix
async def transform(line):
    await asyncio.sleep(0.02)
    result = f"PROCESSED: {line.upper()}"
    print(f"   🔧 {line:20s} → {result}")
    yield result


@task(buffer=0, workers=1)
async def load(item):
    with open("output.txt", "a") as f:
        f.write(item + "\n")
    print(f"   💾 {item}")


if __name__ == "__main__":
    print("ETL Pipeline Verification")
    print("=" * 50)

    setup_files()

    pipeline = extract >> transform >> load
    print("\n🚀 Running pipeline...")
    run()

    success = verify_results()

    # Cleanup
    # os.remove("input.txt")
    # os.remove("output.txt")

    print("\n" + "=" * 50)
    exit(0 if success else 1)
