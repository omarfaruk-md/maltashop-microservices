#!/usr/bin/env bash
# Places one good order and one order whose payment is declined, then checks the final states.
set -euo pipefail
BASE=${BASE:-http://localhost:8080}

place() {  # $1 = payment token, $2 = idempotency key
  curl -s -X POST "$BASE/orders" -H "Content-Type: application/json" \
    -H "Idempotency-Key: $2" -H "X-Correlation-ID: demo-$2" \
    -d "{\"customer_id\":\"c-1\",\"customer_email\":\"maria@example.com\",\"payment_token\":\"$1\",
         \"items\":[{\"sku\":\"SKU-KETTLE\",\"qty\":1,\"unit_price_cents\":2999}]}"
}
wait_for() {  # $1 = order id, $2 = expected status
  for _ in $(seq 1 30); do
    status=$(curl -s "$BASE/orders/$1" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
    [ "$status" = "$2" ] && { echo "order $1 -> $status"; return 0; }
    sleep 1
  done
  echo "FAILED: order $1 did not reach $2 (last status: $status)"; exit 1
}

good=$(place tok_visa "good-$RANDOM"); echo "$good"
wait_for "$(echo "$good" | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")" CONFIRMED
bad=$(place tok_decline "bad-$RANDOM"); echo "$bad"
wait_for "$(echo "$bad" | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")" CANCELLED
echo "smoke test passed"
