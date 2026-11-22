import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.dirname(__file__))
import handlers  # noqa: E402
import service_base as sb  # noqa: E402
from contract_utils import load_contract, violations  # noqa: E402

ORDER = {"customer_id": "c1", "customer_email": "a@example.com", "payment_token": "tok_visa",
         "items": [{"sku": "SKU-KETTLE", "qty": 2, "unit_price_cents": 2500}]}


def msg(msg_type, order_id):
    return {"message_id": sb.make_message(msg_type, {})["message_id"], "type": msg_type,
            "correlation_id": "cid-1", "data": {"order_id": order_id}}


class OrderSagaTests(unittest.TestCase):
    def setUp(self):
        self.db = sb.Database(os.path.join(tempfile.mkdtemp(), "o.db"), handlers.SCHEMA)

    def outbox_types(self):
        with self.db.tx() as c:
            return [r["routing_key"] for r in c.execute("SELECT routing_key FROM outbox ORDER BY created_at, rowid")]

    def place(self, key=None):
        with self.db.tx() as c:
            return handlers.create_order(c, ORDER, key)

    def test_create_order_writes_order_and_two_outbox_messages_atomically(self):
        order, created = self.place()
        self.assertTrue(created)
        self.assertEqual(order["total_cents"], 5000)
        self.assertEqual(self.outbox_types(), ["order.placed", "inventory.reserve"])

    def test_same_idempotency_key_returns_same_order(self):
        first, _ = self.place("key-1")
        second, created = self.place("key-1")
        self.assertFalse(created)
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(len(self.outbox_types()), 2)

    def test_happy_path_confirms_order(self):
        oid, _ = self.place()[0]["id"], None
        reg = handlers.REGISTRY
        sb.handle_delivery(self.db, reg, json.dumps(msg("inventory.reserved", oid)), sb.get_logger("t"))
        sb.handle_delivery(self.db, reg, json.dumps(msg("payment.completed", oid)), sb.get_logger("t"))
        with self.db.tx() as c:
            self.assertEqual(c.execute("SELECT status FROM orders").fetchone()["status"], "CONFIRMED")
        self.assertIn("payment.charge", self.outbox_types())
        self.assertIn("order.confirmed", self.outbox_types())

    def test_duplicate_reply_does_not_send_second_payment_command(self):
        oid = self.place()[0]["id"]
        m = json.dumps(msg("inventory.reserved", oid))
        log = sb.get_logger("t")
        self.assertEqual(sb.handle_delivery(self.db, handlers.REGISTRY, m, log), "processed")
        self.assertEqual(sb.handle_delivery(self.db, handlers.REGISTRY, m, log), "duplicate")
        # a *different* message id for the same reply is also harmless (compare-and-set guard)
        sb.handle_delivery(self.db, handlers.REGISTRY, json.dumps(msg("inventory.reserved", oid)), log)
        self.assertEqual(self.outbox_types().count("payment.charge"), 1)

    def test_payment_failure_triggers_compensation(self):
        oid = self.place()[0]["id"]
        log = sb.get_logger("t")
        sb.handle_delivery(self.db, handlers.REGISTRY, json.dumps(msg("inventory.reserved", oid)), log)
        sb.handle_delivery(self.db, handlers.REGISTRY, json.dumps(msg("payment.failed", oid)), log)
        with self.db.tx() as c:
            self.assertEqual(c.execute("SELECT status FROM orders").fetchone()["status"], "CANCELLED")
        self.assertIn("inventory.release", self.outbox_types())

    def test_out_of_order_payment_reply_is_ignored(self):
        oid = self.place()[0]["id"]
        sb.handle_delivery(self.db, handlers.REGISTRY, json.dumps(msg("payment.completed", oid)), sb.get_logger("t"))
        with self.db.tx() as c:
            self.assertEqual(c.execute("SELECT status FROM orders").fetchone()["status"], "PENDING")

    def test_validation_rejects_bad_input(self):
        self.assertTrue(handlers.validate_order({}))
        self.assertEqual(handlers.validate_order(ORDER), [])


class PaymentChargeContractTest(unittest.TestCase):
    """Contract test (provider side): the message Order really builds satisfies what Payment expects."""

    def test_order_payment_charge_message_matches_contract(self):
        db = sb.Database(os.path.join(tempfile.mkdtemp(), "c.db"), handlers.SCHEMA)
        log = sb.get_logger("t")
        with db.tx() as c:
            oid = handlers.create_order(c, ORDER)[0]["id"]
        sb.handle_delivery(db, handlers.REGISTRY, json.dumps(msg("inventory.reserved", oid)), log)
        with db.tx() as c:
            body = c.execute("SELECT body FROM outbox WHERE routing_key='payment.charge'").fetchone()["body"]
        contract = load_contract("payment.charge.v1.json")
        self.assertEqual(violations(contract["required"], json.loads(body)), [])


if __name__ == "__main__":
    unittest.main()
