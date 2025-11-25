# tests/test_webhook.py
from fastapi.testclient import TestClient
from pydantic import BaseModel
from ezl.core import webhook, Task


# create a simple pydantic model
class MyModel(BaseModel):
    id: int
    name: str


# create webhook Task (decorator produces Task with is_webhook True)
@webhook(
    path="/ingest",
    buffer=2,
    api_key="secret",
    model=MyModel,
    rate_limit_per_minute=2,
)
def my_webhook():
    pass


# small consumer to attach downstream
collected = []


def consumer(item):
    collected.append(item)


def build_client_and_app(wbt: Task):
    # build FastAPI app with the webhook's internal builder to avoid running uvicorn
    app = wbt._build_fastapi(
        wbt.webhook_conf["path"],
        api_key=wbt.webhook_conf.get("api_key"),
        model=wbt.webhook_conf.get("model"),
        rate_limit_per_minute=wbt.webhook_conf.get(
            "rate_limit_per_minute"
        ),
    )
    client = TestClient(app)
    return client, app


def test_webhook_single_accepts_and_validates():
    global collected
    collected = []

    # attach a downstream with a consumer so queue doesn't fill
    downstream = Task(lambda x: None, buffer=10, workers=0)
    my_webhook.downstream = downstream

    client, app = build_client_and_app(my_webhook)
    r = client.post(
        "/ingest",
        json={"id": 1, "name": "a"},
        headers={"X-API-Key": "secret"},
    )
    assert r.status_code == 202
    assert r.json()["accepted"] == 1


def test_webhook_rejects_invalid_json_or_model():
    client, app = build_client_and_app(my_webhook)
    r = client.post(
        "/ingest",
        data="not-json",
        headers={"X-API-Key": "secret"},
    )
    assert r.status_code == 400

    r2 = client.post(
        "/ingest",
        json={"id": "bad", "name": "x"},
        headers={"X-API-Key": "secret"},
    )
    assert r2.status_code == 422


def test_webhook_array_to_single_returns_400():
    client, app = build_client_and_app(my_webhook)
    r = client.post(
        "/ingest",
        json=[{"id": 1, "name": "a"}],
        headers={"X-API-Key": "secret"},
    )
    assert r.status_code == 400


def test_webhook_api_key_enforced():
    client, app = build_client_and_app(my_webhook)
    r = client.post("/ingest", json={"id": 2, "name": "b"})
    assert r.status_code == 401


def test_webhook_rate_limit():
    client, app = build_client_and_app(my_webhook)
    headers = {"X-API-Key": "secret"}
    r1 = client.post(
        "/ingest",
        json={"id": 10, "name": "z"},
        headers=headers,
    )
    assert r1.status_code == 202
    r2 = client.post(
        "/ingest",
        json={"id": 11, "name": "z"},
        headers=headers,
    )
    assert r2.status_code == 202
    # the decorator rate_limit_per_minute=2 should cause next call to be 429
    r3 = client.post(
        "/ingest",
        json={"id": 12, "name": "z"},
        headers=headers,
    )
    assert r3.status_code == 429


def test_webhook_queue_full_returns_503():
    # create a fresh webhook Task and small downstream to fill
    @webhook(
        path="/tiny", buffer=1, api_key=None, model=MyModel
    )
    def tiny():
        pass

    # downstream with buffer 1 and no consumer -> will fill quickly
    downstream = Task(lambda x: None, buffer=1, workers=0)
    tiny.downstream = downstream

    client, app = build_client_and_app(tiny)
    # first post accepted
    r1 = client.post("/tiny", json={"id": 1, "name": "x"})
    assert r1.status_code == 202
    # second post should hit queue.Full and return 503
    r2 = client.post("/tiny", json={"id": 2, "name": "y"})
    assert r2.status_code == 503
