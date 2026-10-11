"""Advisory risk review for a transfer. It never blocks or changes money: a model
must not decide whether a payment goes through.

The facts are computed from the database. A local Qwen model (via Ollama) judges them;
if the model is down, slow, or returns something invalid, a fixed rules script scores
the same facts instead.
"""
import json
import logging

import httpx

log = logging.getLogger("ledger")
LEVELS = ("low", "medium", "high")


def rules_risk(f: dict) -> dict:
    """Fixed fallback. Each signal adds points; the total picks the level."""
    signals = []
    if f["sender_avg_amount"] and f["amount"] >= 10 * f["sender_avg_amount"]:
        signals.append((2, f"amount is {f['amount'] / f['sender_avg_amount']:.0f}x the sender's usual"))
    if f["sender_transfers_last_10m"] >= 5:
        signals.append((2, f"{f['sender_transfers_last_10m']} transfers in the previous 10 minutes"))
    if f["share_of_balance"] >= 0.9:
        signals.append((1, f"moves {f['share_of_balance']:.0%} of the balance"))
    if f["sender_account_age_s"] < 3600:
        signals.append((1, "sender account is under an hour old"))
    if f["first_time_recipient"]:
        signals.append((1, "first transfer to this recipient"))
    points = sum(p for p, _ in signals)
    risk = "high" if points >= 4 else "medium" if points >= 3 else "low"
    # strongest signals first, so the reason leads with what mattered most
    reason = "; ".join(text for _, text in sorted(signals, key=lambda s: -s[0])) or "no risk signals"
    return {"risk": risk, "reason": reason, "engine": "rules"}


class QwenRiskReviewer:
    """Asks a local Qwen model for a second opinion. Owns the HTTP client and its config."""

    PROMPT = """You review bank transfers for fraud risk. Given these facts about one transfer, reply with JSON only:
{{"risk": "low" | "medium" | "high", "reason": "<one short sentence naming the main signal>"}}
Amounts are in cents. A new account, many transfers in a short time, an amount far above the sender's
usual, emptying the balance, or a first-time recipient each raise risk; several together mean high.

FACTS:
{facts}"""

    def __init__(self, url: str, model: str = "qwen2.5:3b", timeout_s: float = 8.0):
        self.url, self.model = url.rstrip("/"), model
        self._client = httpx.AsyncClient(timeout=timeout_s)

    async def close(self):
        await self._client.aclose()

    @staticmethod
    def parse(text: str) -> dict | None:
        """Validate the model's answer. Anything unexpected returns None (caller falls back to rules)."""
        try:
            out = json.loads(text)
            risk, reason = out["risk"], out["reason"]
        except (ValueError, KeyError, TypeError):
            return None
        if risk not in LEVELS or not isinstance(reason, str) or not reason.strip():
            return None
        return {"risk": risk, "reason": reason.strip()[:200], "engine": "qwen"}

    async def review(self, facts: dict) -> dict | None:
        try:
            resp = await self._client.post(f"{self.url}/api/generate", json={
                "model": self.model, "prompt": self.PROMPT.format(facts=json.dumps(facts)),
                "format": "json", "stream": False, "options": {"temperature": 0},
            })
            resp.raise_for_status()
            result = self.parse(resp.json()["response"])
        except (httpx.HTTPError, KeyError, ValueError) as e:
            log.warning("[Risk] MODEL_FAIL err=%s fallback=rules", type(e).__name__)
            return None
        if result is None:
            log.warning("[Risk] MODEL_BAD_OUTPUT fallback=rules")
        return result
