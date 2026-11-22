import os

import handlers
import service_base as sb

URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
log = sb.get_logger("payment-service")
db = sb.Database(os.getenv("DB_PATH", "/data/payments.db"), handlers.SCHEMA)
sb.run_worker(db, URL, log, "payment-service.commands", list(handlers.REGISTRY), handlers.REGISTRY)
