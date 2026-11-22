import os

import handlers
import service_base as sb

URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
db = sb.Database(os.getenv("DB_PATH", "/data/notifications.db"), handlers.SCHEMA)
sb.run_worker(db, URL, handlers.log, "notification-service.events", list(handlers.REGISTRY), handlers.REGISTRY)
