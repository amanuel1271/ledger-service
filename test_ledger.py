import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def account(client, balance):
    return client.post("/accounts", json={"owner": "test", "opening_balance": balance}).json()["id"]


def transfer(client, from_id, to_id, amount, key=None):
    return client.post(
        "/transfers",
        json={"from_id": from_id, "to_id": to_id, "amount": amount},
        headers={"Idempotency-Key": key or str(uuid.uuid4())},
    )


def balance(client, account_id):
    return client.get(f"/accounts/{account_id}").json()["balance"]


def test_transfer_moves_money(client):
    a, b = account(client, 1000), account(client, 0)
    assert transfer(client, a, b, 250).status_code == 201
    assert (balance(client, a), balance(client, b)) == (750, 250)


def test_same_idempotency_key_charges_once(client):
    a, b = account(client, 1000), account(client, 0)
    first = transfer(client, a, b, 100, key="k-" + str(uuid.uuid4()))
    retry = transfer(client, a, b, 100, key=first.json()["idempotency_key"])
    assert retry.json()["id"] == first.json()["id"]
    assert balance(client, a) == 900


def test_reused_key_with_different_body_is_rejected(client):
    a, b = account(client, 1000), account(client, 0)
    key = str(uuid.uuid4())
    transfer(client, a, b, 100, key=key)
    assert transfer(client, a, b, 999, key=key).status_code == 409


def test_overdraft_is_rejected_and_nothing_moves(client):
    a, b = account(client, 50), account(client, 0)
    assert transfer(client, a, b, 51).status_code == 422
    assert (balance(client, a), balance(client, b)) == (50, 0)


def test_concurrent_transfers_never_overdraw(client):
    a, b = account(client, 1000), account(client, 1000)
    # 200 transfers of 10 in both directions at once: money is conserved, no deadlock.
    jobs = [(a, b) if i % 2 else (b, a) for i in range(200)]
    with ThreadPoolExecutor(20) as pool:
        codes = list(pool.map(lambda pair: transfer(client, *pair, 10).status_code, jobs))
    assert set(codes) <= {201, 422}
    assert balance(client, a) + balance(client, b) == 2000
    assert balance(client, a) >= 0 and balance(client, b) >= 0


def test_websocket_streams_transfer_events(client):
    a, b = account(client, 100), account(client, 0)
    with client.websocket_connect(f"/ws/accounts/{b}") as ws:
        assert ws.receive_json()["type"] == "subscribed"
        transfer(client, a, b, 40)
        event = ws.receive_json()
    assert (event["type"], event["to_id"], event["amount"]) == ("transfer", b, 40)
