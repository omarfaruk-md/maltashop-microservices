"""Order service: order creation and the saga ORCHESTRATOR (all business logic, no I/O wiring)."""
import json
import uuid

from service_base import correlation_id_var, enqueue, now

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE, customer_id TEXT, customer_email TEXT,
    items TEXT NOT NULL, total_cents INTEGER NOT NULL, payment_token TEXT NOT NULL,
    status TEXT NOT NULL, reason TEXT, correlation_id TEXT, created_at TEXT, updated_at TEXT);
"""


def validate_order(payload):
    errors = []
    if not payload.get("customer_id"):
        errors.append("customer_id is required")
    if "@" not in str(payload.get("customer_email", "")):
        errors.append("customer_email must be a valid address")
    if not payload.get("payment_token"):
        errors.append("payment_token is required")
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        errors.append("items must be a non-empty list")
    else:
        for item in items:
            if not (isinstance(item, dict) and item.get("sku")
                    and isinstance(item.get("qty"), int) and item["qty"] > 0
                    and isinstance(item.get("unit_price_cents"), int) and item["unit_price_cents"] >= 0):
                errors.append("each item needs sku, positive integer qty and unit_price_cents")
                break
    return errors


def create_order(conn, payload, idempotency_key=None):
    """Returns (order_row_as_dict, created_flag). Safe to call twice with the same key."""
    if idempotency_key:
        row = conn.execute("SELECT * FROM orders WHERE idempotency_key=?", (idempotency_key,)).fetchone()
        if row:
            return dict(row), False
    order_id = str(uuid.uuid4())
    total = sum(i["qty"] * i["unit_price_cents"] for i in payload["items"])
    conn.execute(
        "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (order_id, idempotency_key, payload["customer_id"], payload["customer_email"],
         json.dumps(payload["items"]), total, payload["payment_token"], "PENDING", None,
         correlation_id_var.get(), now(), now()))
    # Event for other services AND the first saga command, written atomically with the order.
    enqueue(conn, "order.placed", {"order_id": order_id, "customer_id": payload["customer_id"],
                                   "customer_email": payload["customer_email"], "total_cents": total})
    enqueue(conn, "inventory.reserve", {"order_id": order_id,
                                        "items": [{"sku": i["sku"], "qty": i["qty"]} for i in payload["items"]]})
    return dict(conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()), True


def _transition(conn, order_id, expected, new, reason=None):
    """Compare-and-set state change: duplicates and out-of-order replies change nothing."""
    cur = conn.execute("UPDATE orders SET status=?, reason=?, updated_at=? WHERE id=? AND status=?",
                       (new, reason, now(), order_id, expected))
    return cur.rowcount == 1


def _order(conn, order_id):
    return dict(conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone())


def on_inventory_reserved(conn, msg):
    oid = msg["data"]["order_id"]
    if _transition(conn, oid, "PENDING", "STOCK_RESERVED"):
        order = _order(conn, oid)
        enqueue(conn, "payment.charge", {"order_id": oid, "amount_cents": order["total_cents"],
                                         "payment_token": order["payment_token"]})


def on_inventory_rejected(conn, msg):
    oid = msg["data"]["order_id"]
    if _transition(conn, oid, "PENDING", "CANCELLED", msg["data"].get("reason")):
        order = _order(conn, oid)
        enqueue(conn, "order.cancelled", {"order_id": oid, "customer_email": order["customer_email"],
                                          "reason": msg["data"].get("reason")})


def on_payment_completed(conn, msg):
    oid = msg["data"]["order_id"]
    if _transition(conn, oid, "STOCK_RESERVED", "CONFIRMED"):
        order = _order(conn, oid)
        enqueue(conn, "order.confirmed", {"order_id": oid, "customer_email": order["customer_email"],
                                          "total_cents": order["total_cents"]})


def on_payment_failed(conn, msg):
    oid = msg["data"]["order_id"]
    if _transition(conn, oid, "STOCK_RESERVED", "CANCELLED", "payment_failed"):
        order = _order(conn, oid)
        # COMPENSATING ACTION: undo the earlier stock reservation, then tell the customer.
        enqueue(conn, "inventory.release", {"order_id": oid})
        enqueue(conn, "order.cancelled", {"order_id": oid, "customer_email": order["customer_email"],
                                          "reason": "payment_failed"})


REGISTRY = {
    "inventory.reserved": on_inventory_reserved,
    "inventory.rejected": on_inventory_rejected,
    "payment.completed": on_payment_completed,
    "payment.failed": on_payment_failed,
}
REPLY_KEYS = list(REGISTRY)
