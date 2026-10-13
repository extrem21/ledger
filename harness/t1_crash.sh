#!/usr/bin/env bash
# T1 — crash harness.
#
# Per trial: seed 50 orders against a live app instance, kill -9 the JVM at
# a random point while the drain worker is (probably) still working through
# the outbox, restart, wait for the queue to settle, and assert every order
# ended up published exactly once with a matching projection row.
#
# The app runs as a plain local `java -jar` process (not docker) so kill -9
# is a direct, unambiguous SIGKILL to the JVM — no shutdown hook runs.
# Postgres runs via docker compose and is NOT reset between trials; results
# accumulate, so each trial's assertion is a global invariant
# (COUNT(orders) == COUNT(outbox WHERE PUBLISHED) == COUNT(order_projection))
# that holds regardless of how many prior trials already contributed rows.
set -uo pipefail
cd "$(dirname "$0")/.."

TRIALS=${TRIALS:-50}
ORDERS_PER_TRIAL=${ORDERS_PER_TRIAL:-50}
APP_PORT=${APP_PORT:-8080}
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
export PATH="$JAVA_HOME/bin:$PATH"
JAVA_BIN="$JAVA_HOME/bin/java"
APP_LOG=/tmp/ledger_t1_app.log
CUSTOMER_ID="8f14e45f-ceea-467a-9e4d-3e2a1b7c9d01"

APP_PID=""
cleanup() {
  if [ -n "$APP_PID" ] && kill -0 "$APP_PID" 2>/dev/null; then
    kill -9 "$APP_PID" 2>/dev/null
  fi
}
trap cleanup EXIT

echo "Building jar..."
"$JAVA_HOME/bin/java" -version 2>&1 | head -1
./mvnw -q -B package -DskipTests || { echo "FAIL: build failed"; exit 1; }
JAR=$(ls target/ledger-*.jar | grep -v original | head -1)
echo "Using $JAR"

echo "Starting fresh Postgres..."
docker compose down -v >/dev/null 2>&1
docker compose up -d postgres >/dev/null 2>&1
until docker compose exec -T postgres pg_isready -U ledger -d ledger >/dev/null 2>&1; do sleep 1; done

psql_query() {
  docker compose exec -T postgres psql -U ledger -d ledger -t -A -F'|' -c "$1" | tr -d '\r'
}

start_app() {
  DB_HOST=localhost DB_PORT=5432 DB_NAME=ledger DB_USER=ledger DB_PASSWORD=ledger \
    SERVER_PORT=$APP_PORT \
    "$JAVA_BIN" -Dledger.outbox.batch-size=10 -jar "$JAR" > "$APP_LOG" 2>&1 &
  APP_PID=$!
}

wait_healthy() {
  local deadline=$((SECONDS + 30))
  while [ $SECONDS -lt $deadline ]; do
    code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$APP_PORT/actuator/health")
    [ "$code" = "200" ] && return 0
    sleep 0.2
  done
  return 1
}

seed_orders() {
  seq 1 "$ORDERS_PER_TRIAL" | xargs -P 20 -I{} curl -s -o /dev/null -X POST \
    "http://localhost:$APP_PORT/orders" \
    -H "Idempotency-Key: t1-$(date +%s%N)-{}-$RANDOM" \
    -H "Content-Type: application/json" \
    -d "{\"customerId\":\"$CUSTOMER_ID\",\"currency\":\"INR\",\"items\":[{\"sku\":\"X\",\"quantity\":1,\"unitPriceCents\":100}]}"
}

wait_settled() {
  local deadline=$((SECONDS + 30))
  while [ $SECONDS -lt $deadline ]; do
    pending=$(curl -s "http://localhost:$APP_PORT/internal/outbox/stats" | grep -oE '"pending":[0-9]+' | grep -oE '[0-9]+')
    [ "$pending" = "0" ] && return 0
    sleep 0.1
  done
  return 1
}

PASS=0
RECOVERY_TIMES=()

for trial in $(seq 1 "$TRIALS"); do
  start_app
  if ! wait_healthy; then
    echo "trial $trial: FAIL (app did not come up)"
    kill -9 "$APP_PID" 2>/dev/null
    wait "$APP_PID" 2>/dev/null
    continue
  fi

  seed_orders

  sleep "0.$(printf '%03d' $((RANDOM % 600)))"
  kill -9 "$APP_PID" 2>/dev/null
  wait "$APP_PID" 2>/dev/null

  restart_ts=$(date +%s.%N)
  start_app
  if ! wait_healthy; then
    echo "trial $trial: FAIL (restart did not come up)"
    continue
  fi

  if wait_settled; then
    settle_ts=$(date +%s.%N)
  else
    settle_ts=$(date +%s.%N)
    echo "trial $trial: FAIL (did not settle within 30s)"
    kill -9 "$APP_PID" 2>/dev/null
    wait "$APP_PID" 2>/dev/null
    continue
  fi
  recovery_ms=$(echo "($settle_ts - $restart_ts) * 1000" | bc | cut -d. -f1)

  counts=$(psql_query "SELECT (SELECT COUNT(*) FROM orders), (SELECT COUNT(*) FROM outbox WHERE state='PUBLISHED'), (SELECT COUNT(*) FROM order_projection), (SELECT COUNT(*) FROM outbox WHERE state='DEAD')")
  orders_count=$(echo "$counts" | cut -d'|' -f1)
  published_count=$(echo "$counts" | cut -d'|' -f2)
  projection_count=$(echo "$counts" | cut -d'|' -f3)
  dead_count=$(echo "$counts" | cut -d'|' -f4)

  if [ "$orders_count" = "$published_count" ] && [ "$orders_count" = "$projection_count" ] && [ "$dead_count" = "0" ]; then
    PASS=$((PASS + 1))
    RECOVERY_TIMES+=("$recovery_ms")
    echo "trial $trial: PASS (recovery ${recovery_ms}ms, orders=$orders_count)"
  else
    echo "trial $trial: FAIL (orders=$orders_count published=$published_count projection=$projection_count dead=$dead_count)"
  fi

  kill -9 "$APP_PID" 2>/dev/null
  wait "$APP_PID" 2>/dev/null
done

median="n/a"
if [ "${#RECOVERY_TIMES[@]}" -gt 0 ]; then
  median=$(printf '%s\n' "${RECOVERY_TIMES[@]}" | sort -n | awk '{a[NR]=$1} END{if(NR%2==1) print a[(NR+1)/2]; else print int((a[NR/2]+a[NR/2+1])/2)}')
fi

echo "-----"
if [ "$PASS" = "$TRIALS" ]; then
  echo "PASS: T1 crash harness ${PASS}/${TRIALS} trials reconciled, median recovery ${median}ms"
  exit 0
else
  echo "FAIL: T1 crash harness ${PASS}/${TRIALS} trials reconciled, median recovery ${median}ms"
  exit 1
fi
