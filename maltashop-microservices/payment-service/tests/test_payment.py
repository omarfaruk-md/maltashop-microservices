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


class PaymentTests(unittest.TestCase):
    def setUp(self):
        self.db = sb.Database(os.path.join(tempfile.mkdtemp(), "p.db"), handlers.SCHEMA)
        self.log = sb.get_logger("t")

    def charge(self, token, mid="m1"):
        m = {"message_id": mid, "type": "payment.charge", "correlation_id": "cid",
             "data": {"order_id": "o1", "amount_cents": 1000, "payment_token": token}}
        return sb.handle_delivery(self.db, handlers.REGISTRY, json.dumps(m), self.log)

    def outbox(self):
        with self.db.tx() as c:
            return [r["routing_key"] for r in c.execute("SELECT routing_key FROM outbox")]

    def test_consumer_side_contract_example_is_accepted(self):
        """Contract test (consumer side): Payment can handle the agreed example message."""
        contract = load_contract("payment.charge.v1.json")
        self.assertEqual(violations(contract["required"], contract["example"]), [])
        sb.handle_delivery(self.db, handlers.REGISTRY, json.dumps(contract["example"]), self.log)
        self.assertEqual(self.outbox(), ["payment.completed"])

    def test_declined_token_publishes_payment_failed(self):
        self.charge("tok_decline")
        self.assertEqual(self.outbox(), ["payment.failed"])

    def test_charge_is_idempotent_per_order(self):
        self.charge("tok_visa", "m1")
        self.assertEqual(self.charge("tok_visa", "m1"), "duplicate")
        self.charge("tok_visa", "m2")  # new message id, same order
        with self.db.tx() as c:
            self.assertEqual(c.execute("SELECT COUNT(*) n FROM payments").fetchone()["n"], 1)


if __name__ == "__main__":
    unittest.main()
