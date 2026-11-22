import os
import uuid

from flask import Flask, jsonify, request

import handlers
import service_base as sb

URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
log = sb.get_logger("order-service")
db = sb.Database(os.getenv("DB_PATH", "/data/orders.db"), handlers.SCHEMA)
app = Flask(__name__)


@app.before_request
def set_correlation_id():
    # Reuse the caller's ID (set by the gateway in production) or create one at the edge.
    sb.correlation_id_var.set(request.headers.get("X-Correlation-ID") or str(uuid.uuid4()))


@app.get("/health")
def health():
    return ("ok", 200) if sb.READY.is_set() else ("not ready", 503)


@app.post("/orders")
def place_order():
    payload = request.get_json(silent=True) or {}
    errors = handlers.validate_order(payload)
    if errors:
        return jsonify(errors=errors), 400
    with db.tx() as conn:
        order, created = handlers.create_order(conn, payload, request.headers.get("Idempotency-Key"))
    log.info("order accepted", extra={"fields": {"order_id": order["id"], "created": created}})
    resp = jsonify(order_id=order["id"], status=order["status"], correlation_id=order["correlation_id"])
    resp.status_code = 202 if created else 200
    resp.headers["X-Correlation-ID"] = order["correlation_id"]
    return resp


@app.get("/orders/<order_id>")
def get_order(order_id):
    with db.tx() as conn:
        row = conn.execute("SELECT id, status, reason, total_cents, correlation_id FROM orders WHERE id=?",
                           (order_id,)).fetchone()
    return (jsonify(dict(row)), 200) if row else (jsonify(error="not found"), 404)


# One gunicorn worker only (see Dockerfile) so that exactly one relay and one consumer run.
sb.start_background(db, URL, log, "order-service.replies", handlers.REPLY_KEYS, handlers.REGISTRY)
