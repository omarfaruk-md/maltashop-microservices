"""Shared messaging helpers.

This file is deliberately COPIED into every service instead of being published as a shared
library, so that services stay independently buildable and deployable (no shared code release).
It provides: JSON logging with a correlation ID, a SQLite helper, a transactional outbox,
an idempotent consumer, and a tiny health endpoint.
"""
import contextvars
import json
import logging
import os
import sqlite3
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

EXCHANGE = "maltashop.events"
DLX = "maltashop.dlx"
correlation_id_var = contextvars.ContextVar("correlation_id", default="-")
READY = threading.Event()  # set once the service is connected to the broker


def now():
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------- structured logging
class JsonFormatter(logging.Formatter):
    def __init__(self, service):
        super().__init__()
        self.service = service

    def format(self, record):
        entry = {
            "timestamp": now(),
            "level": record.levelname,
            "service": self.service,
            "correlation_id": correlation_id_var.get(),
            "message": record.getMessage(),
        }
        entry.update(getattr(record, "fields", {}) or {})
        return json.dumps(entry)


def get_logger(service):
    logger = logging.getLogger(service)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)  # 12-factor: logs are an event stream
        handler.setFormatter(JsonFormatter(service))
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
        logger.propagate = False
    return logger


# ---------------------------------------------------------------- database + outbox
BASE_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
    id TEXT PRIMARY KEY, routing_key TEXT NOT NULL, body TEXT NOT NULL,
    sent INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS processed_messages (
    message_id TEXT PRIMARY KEY, processed_at TEXT NOT NULL);
"""


class Database:
    """One SQLite file per service: the service's private data store."""

    def __init__(self, path, schema=""):
        self.path = path
        conn = sqlite3.connect(path)
        conn.executescript(BASE_SCHEMA + schema)
        conn.close()

    @contextmanager
    def tx(self):
        conn = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()


def make_message(msg_type, data):
    return {"message_id": str(uuid.uuid4()), "type": msg_type, "occurred_at": now(),
            "correlation_id": correlation_id_var.get(), "data": data}


def enqueue(conn, msg_type, data):
    """Transactional outbox: the message is stored in the SAME transaction as the state change."""
    msg = make_message(msg_type, data)
    conn.execute("INSERT INTO outbox(id, routing_key, body, sent, created_at) VALUES (?,?,?,0,?)",
                 (msg["message_id"], msg_type, json.dumps(msg), now()))
    return msg


# ---------------------------------------------------------------- broker plumbing
def connect(url, retries=60, delay=2):
    import pika
    for _ in range(retries):
        try:
            return pika.BlockingConnection(pika.URLParameters(url))
        except pika.exceptions.AMQPConnectionError:
            time.sleep(delay)
    raise RuntimeError("RabbitMQ not reachable")


def declare_topology(ch):
    ch.exchange_declare(EXCHANGE, "topic", durable=True)
    ch.exchange_declare(DLX, "fanout", durable=True)
    ch.queue_declare("dead-letters", durable=True)
    ch.queue_bind("dead-letters", DLX)


def relay_loop(db, url, log, interval=1.0):
    """Publishes unsent outbox rows with publisher confirms (at-least-once delivery)."""
    import pika
    conn = None
    while True:
        try:
            if conn is None or conn.is_closed:
                conn = connect(url)
                ch = conn.channel()
                ch.confirm_delivery()
                declare_topology(ch)
            with db.tx() as c:
                rows = c.execute("SELECT id, routing_key, body FROM outbox WHERE sent=0 "
                                 "ORDER BY created_at LIMIT 50").fetchall()
            for row in rows:
                msg = json.loads(row["body"])
                ch.basic_publish(EXCHANGE, row["routing_key"], row["body"],
                                 pika.BasicProperties(delivery_mode=2, content_type="application/json",
                                                      message_id=row["id"],
                                                      correlation_id=msg["correlation_id"]),
                                 mandatory=True)  # unroutable (queue not yet declared) -> exception -> retry later
                with db.tx() as c:
                    c.execute("UPDATE outbox SET sent=1 WHERE id=?", (row["id"],))
                token = correlation_id_var.set(msg["correlation_id"])
                log.info("message published", extra={"fields": {"event": msg["type"]}})
                correlation_id_var.reset(token)
            conn.sleep(interval)
        except Exception as exc:  # reconnect on any broker error
            log.error("relay error: %s", exc)
            conn = None
            time.sleep(2)


def handle_delivery(db, registry, body, log):
    """Process one message. Idempotent: a message_id is applied at most once."""
    msg = json.loads(body)
    token = correlation_id_var.set(msg.get("correlation_id", "-"))
    try:
        with db.tx() as conn:
            seen = conn.execute("SELECT 1 FROM processed_messages WHERE message_id=?",
                                (msg["message_id"],)).fetchone()
            if seen:
                log.info("duplicate message ignored", extra={"fields": {"event": msg["type"]}})
                return "duplicate"
            handler = registry.get(msg["type"])
            if handler is None:
                log.warning("no handler for message type", extra={"fields": {"event": msg["type"]}})
            else:
                log.info("message received", extra={"fields": {"event": msg["type"]}})
                handler(conn, msg)
            # same transaction as the handler's effects -> effect and "seen" marker commit together
            conn.execute("INSERT INTO processed_messages(message_id, processed_at) VALUES (?,?)",
                         (msg["message_id"], now()))
        return "processed"
    finally:
        correlation_id_var.reset(token)


def consume_loop(db, url, queue, routing_keys, registry, log):
    while True:
        try:
            conn = connect(url)
            ch = conn.channel()
            declare_topology(ch)
            ch.queue_declare(queue, durable=True, arguments={"x-dead-letter-exchange": DLX})
            for key in routing_keys:
                ch.queue_bind(queue, EXCHANGE, key)
            ch.basic_qos(prefetch_count=10)

            def on_message(channel, method, props, body):
                try:
                    handle_delivery(db, registry, body, log)
                    channel.basic_ack(method.delivery_tag)  # ack only AFTER the commit
                except Exception as exc:
                    log.error("handler failed, sending to dead-letter queue: %s", exc)
                    channel.basic_nack(method.delivery_tag, requeue=False)

            ch.basic_consume(queue, on_message)
            READY.set()
            ch.start_consuming()
        except Exception as exc:
            READY.clear()
            log.error("consumer error: %s", exc)
            time.sleep(2)


# ---------------------------------------------------------------- health endpoint (workers)
def start_health_server(port=8000):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200 if READY.is_set() else 503)
            self.end_headers()
            self.wfile.write(b"ok" if READY.is_set() else b"not ready")

        def log_message(self, *args):
            pass

    threading.Thread(target=HTTPServer(("0.0.0.0", port), Handler).serve_forever, daemon=True).start()


def start_background(db, url, log, queue, routing_keys, registry):
    threading.Thread(target=relay_loop, args=(db, url, log), daemon=True).start()
    threading.Thread(target=consume_loop, args=(db, url, queue, routing_keys, registry, log),
                     daemon=True).start()


def run_worker(db, url, log, queue, routing_keys, registry):
    """Entry point for worker-only services (no REST API)."""
    start_health_server(int(os.getenv("HEALTH_PORT", "8000")))
    threading.Thread(target=relay_loop, args=(db, url, log), daemon=True).start()
    consume_loop(db, url, queue, routing_keys, registry, log)
