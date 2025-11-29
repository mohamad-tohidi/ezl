# examples/or_condition.py
from ezl import task, run


@task(workers=0)
async def extractor():
    for i in range(10):
        yield {"id": i, "value": i}


@task(workers=4)
def even(item):
    return {**item, "path": "EVEN"}


@task(workers=4)
def odd(item):
    return {**item, "path": "ODD"}


@task(workers=0)
async def load(item):
    print(f"→ {item['path']} | id={item['id']}")


def is_even(item):
    return item["value"] % 2 == 0


if __name__ == "__main__":
    run(extractor >> ((even | odd) ^ is_even) >> load)
