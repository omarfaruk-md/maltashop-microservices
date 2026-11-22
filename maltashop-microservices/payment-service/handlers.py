"""Payment service: a STUB of the external payment provider integration (saga participant).

No card data is handled. The client sends a provider token; the token 'tok_decline' simulates a
declined card so that the compensating path of the saga can be demonstrated.
"""
from service_base import enqueue, now

SCHEMA = """
CREATE TABLE IF NOT EXISTS payments (
    order_id TEXT PRIMARY KEY, amount_cents INTEGER NOT NULL, status TEXT NOT NULL, created_at TEXT);
"""


def on_charge(conn, msg):
    d = msg["data"]
    existing = conn.execute("SELECT status FROM payments WHERE order_id=?", (d["order_id"],)).fetchone()
    status = existing["status"] if existing else (
        "FAILED" if d["payment_token"] == "tok_decline" else "COMPLETED")
    if not existing:  # one payment record per order: repeated commands do not charge twice
        conn.execute("INSERT INTO payments VALUES (?,?,?,?)", (d["order_id"], d["amount_cents"], status, now()))
    enqueue(conn, "payment.completed" if status == "COMPLETED" else "payment.failed",
            {"order_id": d["order_id"]})


REGISTRY = {"payment.charge": on_charge}
