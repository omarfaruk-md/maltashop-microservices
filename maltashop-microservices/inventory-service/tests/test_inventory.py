import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import handlers  # noqa: E402
import service_base as sb  # noqa: E402


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.db = sb.Database(os.path.join(tempfile.mkdtemp(), "i.db"), handlers.SCHEMA)
        handlers.seed(self.db)
        self.log = sb.get_logger("t")
        self.n = 0

    def send(self, msg_type, qty=1, sku="SKU-LAPTOP", mid=None):
        self.n += 1
        data = {"order_id": "o1", "items": [{"sku": sku, "qty": qty}]}
        m = {"message_id": mid or f"m{self.n}", "type": msg_type, "correlation_id": "cid", "data": data}
        sb.handle_delivery(self.db, handlers.REGISTRY, json.dumps(m), self.log)

    def stock(self, sku="SKU-LAPTOP"):
        with self.db.tx() as c:
            return c.execute("SELECT available FROM stock WHERE sku=?", (sku,)).fetchone()["available"]

    def outbox(self):
        with self.db.tx() as c:
            return [r["routing_key"] for r in c.execute("SELECT routing_key FROM outbox ORDER BY rowid")]

    def test_reserve_decrements_stock(self):
        self.send("inventory.reserve", qty=2)
        self.assertEqual(self.stock(), 3)
        self.assertEqual(self.outbox(), ["inventory.reserved"])

    def test_insufficient_stock_is_rejected_without_change(self):
        self.send("inventory.reserve", qty=99)
        self.assertEqual(self.stock(), 5)
        self.assertEqual(self.outbox(), ["inventory.rejected"])

    def test_repeated_reserve_for_same_order_reserves_once(self):
        self.send("inventory.reserve", qty=2)
        self.send("inventory.reserve", qty=2)  # different message id, same order
        self.assertEqual(self.stock(), 3)

    def test_release_is_compensation_and_idempotent(self):
        self.send("inventory.reserve", qty=2)
        self.send("inventory.release")
        self.send("inventory.release")
        self.assertEqual(self.stock(), 5)


if __name__ == "__main__":
    unittest.main()
