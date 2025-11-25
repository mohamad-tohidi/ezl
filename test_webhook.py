from src.ezl.core import webhook, task, run
from pydantic import BaseModel
import time


class DocModel(BaseModel):
    id: str
    text: str
    priority: str


# secret used for HMAC
# HMAC_SECRET = b"supersecret"


@webhook(
    path="/ingest",
    host="127.0.0.1",
    port=9000,
    # api_key="my-api-key-123",
    # hmac_secret=HMAC_SECRET,
    model=DocModel,
    rate_limit_per_minute=30,
    buffer=2,
)
def extract():
    pass


@task(buffer=100, workers=1)
def transform(item):
    time.sleep(2)
    # ... same transform as before
    return item


@task(buffer=50, workers=2)
async def load(item):
    print("Loaded:", item["id"])


pipeline = extract >> transform >> load

if __name__ == "__main__":
    run(pipeline)
