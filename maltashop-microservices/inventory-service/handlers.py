"""Inventory service: stock levels and reservations (a saga participant)."""
import json

from service_base import enqueue, now

SCHEMA = """
CREATE TABLE IF NOT EXISTS stock (sku TEXT PRIMARY KEY, available INTEGER NOT NULL CHECK (available >= 0));
CREATE TABLE IF NOT EXISTS reservations (
    order_id TEXT PRIMARY KEY, items TEXT NOT NULL, status TEXT NOT NULL, updated_at TEXT);
"""
SEED = {"SKU-LAPTOP": 5, "SKU-KETTLE": 20}


def seed(db):
    with db.tx() as conn:
        for sku, qty in SEED.items():
            conn.execute("INSERT OR IGNORE INTO stock(sku, available) VALUES (?,?)", (sku, qty))


def on_reserve(conn, msg):
    order_id, items = msg["data"]["order_id"], msg["data"]["items"]
    existing = conn.execute("SELECT status FROM reservations WHERE order_id=?", (order_id,)).fetchone()
    if existing:  # a repeated command for the same order never reserves twice
        if existing["status"] == "REJECTED":
            enqueue(conn, "inventory.rejected", {"order_id": order_id, "reason": "insufficient_stock"})
        else:
            enqueue(conn, "inventory.reserved", {"order_id": order_id})
        return
    short = [i["sku"] for i in items
             if (conn.execute("SELECT available FROM stock WHERE sku=?", (i["sku"],)).fetchone()
                 or {"available": 0})["available"] < i["qty"]]
    if short:
        conn.execute("INSERT INTO reservations VALUES (?,?,?,?)", (order_id, json.dumps(items), "REJECTED", now()))
        enqueue(conn, "inventory.rejected", {"order_id": order_id, "reason": "insufficient_stock: " + ",".join(short)})
        return
    for i in items:
        conn.execute("UPDATE stock SET available = available - ? WHERE sku=?", (i["qty"], i["sku"]))
    conn.execute("INSERT INTO reservations VALUES (?,?,?,?)", (order_id, json.dumps(items), "RESERVED", now()))
    enqueue(conn, "inventory.reserved", {"order_id": order_id})


def on_release(conn, msg):
    """Compensating action for inventory.reserve: put the reserved stock back."""
    order_id = msg["data"]["order_id"]
    res = conn.execute("SELECT items, status FROM reservations WHERE order_id=?", (order_id,)).fetchone()
    if res and res["status"] == "RESERVED":
        for i in json.loads(res["items"]):
            conn.execute("UPDATE stock SET available = available + ? WHERE sku=?", (i["qty"], i["sku"]))
        conn.execute("UPDATE reservations SET status='RELEASED', updated_at=? WHERE order_id=?", (now(), order_id))


REGISTRY = {"inventory.reserve": on_reserve, "inventory.release": on_release}
