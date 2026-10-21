import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

import time

import json

from app import OutboxRelay, app, transfer_facts
from risk import QwenRiskReviewer, rules_risk


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

    async def ping(self):
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


class FakeReviewer:
    """Stands in for the model: returns a fixed verdict, or None to simulate "model unavailable"."""

    def __init__(self, verdict=None):
        self.verdict = verdict

    async def review(self, facts):
        return self.verdict

    async def close(self):
        pass


@pytest.fixture(autouse=True)
def model_unavailable(client):
    app.state.risk = FakeReviewer(None)  # every test starts on the rules path, like CI


def risk(client, transfer_id):
    return client.get(f"/transfers/{transfer_id}/risk")


def test_ordinary_transfer_is_low_risk(client):
    a, b = account(client, 10_000), account(client, 0)
    t = transfer(client, a, b, 100).json()
    r = risk(client, t["id"]).json()
    assert (r["risk"], r["engine"]) == ("low", "rules")
    assert r["facts"]["first_time_recipient"] is True and r["facts"]["share_of_balance"] == 0.01
    # the real model client json.dumps() the facts; a Decimal here once crashed it while stubs passed
    json.dumps(client.portal.call(transfer_facts, app.state.db, t["id"]))


def test_burst_then_draining_transfer_is_high_risk(client):
    a, b = account(client, 10_000), account(client, 0)
    for _ in range(5):
        transfer(client, a, b, 10)
    t = transfer(client, a, b, 9_950).json()  # 995x the usual amount, empties the account
    r = risk(client, t["id"]).json()
    assert (r["risk"], r["engine"]) == ("high", "rules")
    assert "the sender's usual" in r["reason"] and "5 transfers in the previous 10 minutes" in r["reason"]
    assert r["facts"]["share_of_balance"] == 1.0


def test_risk_review_never_changes_money(client):
    a, b = account(client, 500), account(client, 0)
    t = transfer(client, a, b, 500).json()
    risk(client, t["id"])
    assert (balance(client, a), balance(client, b)) == (0, 500)


def test_unknown_transfer_risk_is_404(client):
    assert risk(client, 10**12).status_code == 404


def test_model_verdict_is_used_and_cached(client):
    a, b = account(client, 1000), account(client, 0)
    t = transfer(client, a, b, 5).json()
    app.state.risk = FakeReviewer({"risk": "high", "reason": "model says so", "engine": "qwen"})
    assert risk(client, t["id"]).json()["engine"] == "qwen"
    app.state.risk = FakeReviewer(None)  # model goes down: the cached verdict still answers
    r = risk(client, t["id"]).json()
    assert (r["risk"], r["reason"], r["engine"]) == ("high", "model says so", "qwen")


def test_invalid_model_output_is_rejected():
    parse = QwenRiskReviewer.parse
    assert parse('{"risk": "high", "reason": "many transfers"}') == {"risk": "high", "reason": "many transfers", "engine": "qwen"}
    for bad in ['not json', '{"risk": "banana", "reason": "x"}', '{"risk": "high"}', '{"risk": "low", "reason": ""}', '["high"]']:
        assert parse(bad) is None, bad


def test_rules_thresholds():
    base = {"amount": 100, "sender_account_age_s": 10**6, "sender_transfers_last_10m": 0,
            "sender_avg_amount": 100, "first_time_recipient": False, "share_of_balance": 0.1}
    assert rules_risk(base) == {"risk": "low", "reason": "no risk signals", "engine": "rules"}
    assert rules_risk({**base, "sender_transfers_last_10m": 5, "first_time_recipient": True})["risk"] == "medium"  # 2+1
    assert rules_risk({**base, "sender_transfers_last_10m": 5, "amount": 1000})["risk"] == "high"  # 2+2
    assert rules_risk({**base, "sender_avg_amount": None, "amount": 10**9})["risk"] == "low"  # no history: no "usual"


# ---------- health and metrics ----------
def metric(client, name, **labels):
    """Value of one Prometheus sample from /metrics (0 if absent)."""
    want = ",".join(f'{k}="{v}"' for k, v in labels.items())
    for line in client.get("/metrics").text.splitlines():
        if line.startswith(name + ("{" + want + "}" if want else " ")):
            return float(line.rsplit(" ", 1)[1])
    return 0.0


def test_health_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok" and body["checks"]["database"] == "ok" and body["checks"]["redis"] == "ok"
    assert body["checks"]["outbox_relay"] == "ok" and isinstance(body["checks"]["outbox_backlog"], int)


def test_health_is_degraded_not_down_when_redis_is_down(client):
    real = app.state.redis
    app.state.redis = RedisDown()  # the relay keeps its own client; only the health probe sees the outage
    try:
        r = client.get("/health")
    finally:
        app.state.redis = real
    assert r.status_code == 200 and r.json()["status"] == "degraded"
    assert r.json()["checks"]["redis"].startswith("down")


def test_health_is_503_when_database_is_down(client):
    class DbDown:
        async def fetchval(self, *args):
            raise ConnectionError("postgres is down")
    real = app.state.db
    app.state.db = DbDown()
    try:
        r = client.get("/health")
    finally:
        app.state.db = real
    assert r.status_code == 503 and r.json()["status"] == "down"


def test_metrics_count_transfer_outcomes(client):
    before = {o: metric(client, "ledger_transfers_total", outcome=o) for o in ("created", "replayed", "insufficient_funds")}
    a, b = account(client, 100), account(client, 0)
    key = "m-" + str(uuid.uuid4())
    assert transfer(client, a, b, 10, key=key).status_code == 201
    assert transfer(client, a, b, 10, key=key).status_code == 201  # replay
    assert transfer(client, a, b, 1000).status_code == 422
    assert metric(client, "ledger_transfers_total", outcome="created") == before["created"] + 1
    assert metric(client, "ledger_transfers_total", outcome="replayed") == before["replayed"] + 1
    assert metric(client, "ledger_transfers_total", outcome="insufficient_funds") == before["insufficient_funds"] + 1


def test_metrics_use_route_templates_not_raw_ids(client):
    a = account(client, 0)
    client.get(f"/accounts/{a}")
    text = client.get("/metrics").text
    assert 'route="/accounts/{account_id}"' in text
    assert f'route="/accounts/{a}"' not in text  # raw ids would explode label cardinality
    assert "ledger_outbox_backlog" in text and "http_request_duration_seconds_bucket" in text

