import os

import handlers
import service_base as sb

URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
log = sb.get_logger("inventory-service")
db = sb.Database(os.getenv("DB_PATH", "/data/inventory.db"), handlers.SCHEMA)
handlers.seed(db)
sb.run_worker(db, URL, log, "inventory-service.commands", list(handlers.REGISTRY), handlers.REGISTRY)
