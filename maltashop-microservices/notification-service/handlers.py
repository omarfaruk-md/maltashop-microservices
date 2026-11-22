"""Notification service: sends (simulated) emails. Failure here can never affect checkout."""
import uuid

from service_base import get_logger, now

log = get_logger("notification-service")
SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications (
    id TEXT PRIMARY KEY, order_id TEXT, kind TEXT, recipient TEXT, created_at TEXT);
"""
SUBJECTS = {"order.placed": "We received your order",
            "order.confirmed": "Your order is confirmed",
            "order.cancelled": "Your order was cancelled"}


def on_order_event(conn, msg):
    d = msg["data"]
    conn.execute("INSERT INTO notifications VALUES (?,?,?,?,?)",
                 (str(uuid.uuid4()), d["order_id"], msg["type"], d.get("customer_email"), now()))
    log.info("email sent (simulated)", extra={"fields": {"order_id": d["order_id"],
                                                         "subject": SUBJECTS[msg["type"]]}})


REGISTRY = {key: on_order_event for key in SUBJECTS}
