import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import handlers  # noqa: E402
import service_base as sb  # noqa: E402


class NotificationTests(unittest.TestCase):
    def test_one_notification_per_message_even_if_redelivered(self):
        db = sb.Database(os.path.join(tempfile.mkdtemp(), "n.db"), handlers.SCHEMA)
        m = json.dumps({"message_id": "m1", "type": "order.confirmed", "correlation_id": "cid",
                        "data": {"order_id": "o1", "customer_email": "a@example.com"}})
        sb.handle_delivery(db, handlers.REGISTRY, m, handlers.log)
        sb.handle_delivery(db, handlers.REGISTRY, m, handlers.log)
        with db.tx() as c:
            self.assertEqual(c.execute("SELECT COUNT(*) n FROM notifications").fetchone()["n"], 1)


if __name__ == "__main__":
    unittest.main()
