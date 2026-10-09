import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

import time

from app import OutboxRelay, app


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
    assert isinstance(event["event_id"], int)  # lets consumers drop a rare duplicate


def outbox_count(client):
    return client.portal.call(app.state.db.fetchval, "SELECT count(*) FROM outbox")


class RedisDown:
    async def publish(self, *args):
        raise ConnectionError("redis is down")


def test_events_survive_a_redis_outage(client):
    app.state.relay.pause()
    try:
        a, b = account(client, 100), account(client, 0)
        before = outbox_count(client)
        assert transfer(client, a, b, 10).status_code == 201
        assert outbox_count(client) == before + 2  # one event per account, committed with the transfer
        try:
            client.portal.call(OutboxRelay(app.state.db, RedisDown()).run_once)
        except ConnectionError:
            pass
        assert outbox_count(client) == before + 2  # publish failed -> nothing deleted
    finally:
        app.state.relay.resume()
    deadline = time.time() + 3
    while outbox_count(client) and time.time() < deadline:  # relay recovers on its own
        time.sleep(0.05)
    assert outbox_count(client) == 0


def test_rejected_transfer_writes_no_events(client):
    app.state.relay.pause()
    try:
        a, b = account(client, 5), account(client, 0)
        before = outbox_count(client)
        assert transfer(client, a, b, 6).status_code == 422
        assert outbox_count(client) == before
    finally:
        app.state.relay.resume()


def move(client, account_id, kind, amount, key=None):
    return client.post(f"/accounts/{account_id}/{kind}", json={"amount": amount},
                       headers={"Idempotency-Key": key or str(uuid.uuid4())})


def ledger_sum(client, account_id):
    return client.portal.call(app.state.db.fetchval,
                              "SELECT COALESCE(SUM(amount), 0) FROM entries WHERE account_id = $1", account_id)


def test_deposit_and_withdrawal(client):
    a = account(client, 100)
    assert move(client, a, "deposits", 50).status_code == 201
    assert move(client, a, "withdrawals", 30).status_code == 201
    assert balance(client, a) == 120


def test_withdrawal_cannot_overdraw(client):
    a = account(client, 10)
    assert move(client, a, "withdrawals", 11).status_code == 422
    assert balance(client, a) == 10 and ledger_sum(client, a) == 10


def test_deposit_is_idempotent(client):
    a = account(client, 0)
    key = str(uuid.uuid4())
    first, retry = move(client, a, "deposits", 25, key), move(client, a, "deposits", 25, key)
    assert retry.json()["id"] == first.json()["id"] and balance(client, a) == 25
    assert move(client, a, "deposits", 99, key).status_code == 409  # same key, different amount
    assert move(client, a, "withdrawals", 25, key).status_code == 409  # same key, different direction


def test_unknown_account_is_404(client):
    assert move(client, 10**12, "deposits", 5).status_code == 404
    assert client.get(f"/accounts/{10**12}/transactions").status_code == 404


def test_balance_always_equals_ledger(client):
    a, b = account(client, 1000), account(client, 1000)
    jobs = [(a, b) if i % 2 else (b, a) for i in range(100)]
    with ThreadPoolExecutor(20) as pool:
        list(pool.map(lambda pair: transfer(client, *pair, 7), jobs))
        list(pool.map(lambda i: move(client, a, "deposits" if i % 2 else "withdrawals", 3), range(40)))
    for acct in (a, b):
        assert balance(client, acct) == ledger_sum(client, acct)


def test_history_pages_cover_everything_newest_first(client):
    a, b = account(client, 100), account(client, 0)
    move(client, a, "deposits", 10)
    transfer(client, a, b, 20)
    move(client, a, "withdrawals", 5)
    seen, cursor = [], None
    while True:
        page = client.get(f"/accounts/{a}/transactions", params={"limit": 2, **({"cursor": cursor} if cursor else {})}).json()
        seen += page["items"]
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert [e["kind"] for e in seen] == ["withdrawal", "transfer_out", "deposit", "opening"]
    assert [e["amount"] for e in seen] == [-5, -20, 10, 100]
    assert len({e["id"] for e in seen}) == 4  # no duplicates across pages
