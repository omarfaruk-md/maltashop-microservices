# MaltaShop "place an order" prototype

Unit 5 (Service-Oriented Architecture and Microservices Fundamentals) assignment prototype.
It demonstrates an **orchestrated saga** across four independently deployable services that
communicate through RabbitMQ and never share a database.

## What the system is

| Service | Type | Owns (SQLite file in its own volume) | Role |
|---|---|---|---|
| order-service | REST API + worker | `orders`, `outbox`, `processed_messages` | Accepts orders, runs the saga (orchestrator) |
| inventory-service | worker | `stock`, `reservations` | Reserves / releases stock |
| payment-service | worker | `payments` | Stub of the external payment provider |
| notification-service | worker | `notifications` | Sends (simulated) emails |

Message flow (all through the topic exchange `maltashop.events`):
`POST /orders` -> `order.placed` + `inventory.reserve` -> `inventory.reserved` -> `payment.charge`
-> `payment.completed` -> `order.confirmed`. If payment fails: `payment.failed` -> `inventory.release`
(compensation) + `order.cancelled`.

## Run it

Requirements: Docker with Compose v2.

```bash
cp .env.example .env
docker compose up --build
```

Place an order (a second terminal):

```bash
curl -i -X POST http://localhost:8080/orders -H "Content-Type: application/json" \
  -H "X-Correlation-ID: demo-001" -H "Idempotency-Key: order-001" \
  -d '{"customer_id":"c-1","customer_email":"maria@example.com","payment_token":"tok_visa",
       "items":[{"sku":"SKU-KETTLE","qty":1,"unit_price_cents":2999}]}'
curl http://localhost:8080/orders/<order_id>          # PENDING -> STOCK_RESERVED -> CONFIRMED
```

Use `"payment_token":"tok_decline"` to trigger the compensation path (order ends `CANCELLED`).
Seed stock: `SKU-LAPTOP` = 5, `SKU-KETTLE` = 20. RabbitMQ UI: http://localhost:15672 (guest / guest).

Follow one request across services by its correlation ID:

```bash
docker compose logs --no-color | grep demo-001
```

## Tests

```bash
cd order-service && python -m unittest discover -s tests -v     # saga, idempotency, contract (provider)
cd payment-service && python -m unittest discover -s tests -v   # contract (consumer), idempotent charge
./scripts/smoke_test.sh                                          # needs the system running
```

## Environment variables

| Variable | Used by | Default | Meaning |
|---|---|---|---|
| `RABBITMQ_URL` | all | `amqp://guest:guest@rabbitmq:5672/` | Broker connection string |
| `DB_PATH` | all | `/data/<service>.db` | Location of the service's private SQLite file |
| `LOG_LEVEL` | all | `INFO` | Log verbosity |
| `HEALTH_PORT` | workers | `8000` | Port of the `/health` endpoint |

## Known limitations (prototype only)

SQLite instead of PostgreSQL; single broker node; no authentication; payment provider is a stub;
failed messages go to a dead-letter queue with no automatic retry; emails travel in events
(production would look up contact data to minimise personal data in messages).
