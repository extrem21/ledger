#!/usr/bin/env bash
# Benchmark orchestrator:
#  - 30s warmup, discarded
#  - write throughput: POST /orders at 10/25/50/100 VUs, p50/p95/p99
#  - end-to-end lag: commit (orders.created_at) -> projection applied
#    (order_projection.last_updated_at), p50/p99, via Postgres timestamps
#    so there's no cross-host clock skew
#  - drain throughput at 1/2/4 workers, events seeded directly into the
#    outbox to isolate drain speed from write-path speed
#
# Raw output lands in bench/raw/; bench/results.md is written by hand from
# these real runs (see bench/raw/) rather than auto-generated, so numbers
# are never silently mis-parsed.
set -uo pipefail
cd "$(dirname "$0")/.."

export JAVA_HOME=/opt/homebrew/opt/openjdk@21
export PATH="$JAVA_HOME/bin:$PATH"
APP_PORT=${APP_PORT:-8080}
JAVA_BIN="$JAVA_HOME/bin/java"
RAW_DIR=bench/raw
mkdir -p "$RAW_DIR"
rm -f "$RAW_DIR"/*

APP_PID=""
stop_app() {
  if [ -n "$APP_PID" ] && kill -0 "$APP_PID" 2>/dev/null; then
    kill -9 "$APP_PID" 2>/dev/null
    wait "$APP_PID" 2>/dev/null
  fi
}
trap stop_app EXIT

echo "Building jar..."
./mvnw -q -B package -DskipTests
JAR=$(ls target/ledger-*.jar | grep -v original | head -1)

echo "Starting fresh Postgres..."
docker compose down -v >/dev/null 2>&1
docker compose up -d postgres >/dev/null 2>&1
until docker compose exec -T postgres pg_isready -U ledger -d ledger >/dev/null 2>&1; do sleep 1; done

psql_query() {
  docker compose exec -T postgres psql -U ledger -d ledger -t -A -F'|' -c "$1" | tr -d '\r'
}
psql_exec() {
  docker compose exec -T postgres psql -U ledger -d ledger -c "$1" >/dev/null
}

start_app() {
  local workers=$1
  DB_HOST=localhost DB_PORT=5432 DB_NAME=ledger DB_USER=ledger DB_PASSWORD=ledger \
    SERVER_PORT=$APP_PORT \
    "$JAVA_BIN" -Dledger.outbox.worker-count="$workers" -jar "$JAR" > /tmp/ledger_bench_app.log 2>&1 &
  APP_PID=$!
}

wait_healthy() {
  local deadline=$((SECONDS + 30))
  while [ $SECONDS -lt $deadline ]; do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:$APP_PORT/actuator/health")" = "200" ] && return 0
    sleep 0.2
  done
  return 1
}

wait_drained() {
  local timeout=$1
  local deadline=$((SECONDS + timeout))
  while [ $SECONDS -lt $deadline ]; do
    pending=$(curl -s "http://localhost:$APP_PORT/internal/outbox/stats" | grep -oE '"pending":[0-9]+' | grep -oE '[0-9]+')
    [ "$pending" = "0" ] && return 0
    sleep 0.2
  done
  return 1
}

# ---------- Write throughput + e2e lag (worker-count=1, in-process sink) ----------
start_app 1
wait_healthy || { echo "FAIL: app did not start"; exit 1; }

echo "Warming up (30s, discarded)..."
k6 run -e BASE_URL="http://localhost:$APP_PORT" -e VUS=50 -e DURATION=30s \
  bench/write_throughput.js > "$RAW_DIR/warmup.txt" 2>&1

echo "Write throughput benchmark..."
for vus in 10 25 50 100; do
  echo "  VUs=$vus"
  k6 run -e BASE_URL="http://localhost:$APP_PORT" -e VUS="$vus" -e DURATION=20s \
    bench/write_throughput.js > "$RAW_DIR/write_${vus}vus.txt" 2>&1
done

echo "Waiting for the write-throughput backlog to fully drain..."
wait_drained 120

# End-to-end lag is measured as its own light, steady-state pass — not off
# the throughput-ramp data above. At 50-100 VUs the single default worker
# (batch-size=50, 100ms tick, ~500 events/s ceiling) can't keep up with
# write throughput, so a backlog builds; lag measured over that data would
# mostly be "queueing delay behind an artificial burst," not per-event
# pipeline latency. A light load well under drain capacity isolates the
# latter.
echo "Measuring steady-state end-to-end lag (fixed-rate light load, isolated from the burst above)..."
psql_exec "TRUNCATE outbox, processed_events, order_projection, idempotency_keys, order_items, orders RESTART IDENTITY CASCADE"
k6 run -e BASE_URL="http://localhost:$APP_PORT" -e RATE=20 -e DURATION=10s \
  bench/write_throughput.js > "$RAW_DIR/lag_load.txt" 2>&1
wait_drained 30
psql_query "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY lag_ms), percentile_cont(0.99) WITHIN GROUP (ORDER BY lag_ms), count(*) FROM (SELECT EXTRACT(EPOCH FROM (p.last_updated_at - o.created_at)) * 1000 AS lag_ms FROM order_projection p JOIN orders o ON o.id = p.order_id) t" \
  > "$RAW_DIR/e2e_lag.txt"
cat "$RAW_DIR/e2e_lag.txt"

stop_app

# ---------- Drain throughput at 1/2/4 workers ----------
echo "Drain throughput benchmark (1/2/4 workers, 20000 pre-seeded events each)..."
: > "$RAW_DIR/drain_throughput.txt"
for workers in 1 2 4; do
  psql_exec "TRUNCATE outbox, processed_events, order_projection, idempotency_keys, order_items, orders RESTART IDENTITY CASCADE"

  start_app "$workers"
  wait_healthy || { echo "FAIL: app did not start (worker-count=$workers)"; exit 1; }

  N=20000
  echo "  seeding $N events for worker-count=$workers..."
  psql_exec "INSERT INTO outbox (aggregate_id, event_type, payload)
             SELECT gen_random_uuid(), 'OrderCreated',
                    jsonb_build_object('orderId', gen_random_uuid(), 'status', 'PENDING', 'totalCents', 100, 'occurredAt', now())
             FROM generate_series(1, $N)"

  start_ts=$(date +%s.%N)
  if wait_drained 120; then
    end_ts=$(date +%s.%N)
    elapsed=$(echo "$end_ts - $start_ts" | bc)
    rate=$(echo "$N / $elapsed" | bc)
    echo "worker-count=$workers events=$N elapsed=${elapsed}s rate=${rate}/s" | tee -a "$RAW_DIR/drain_throughput.txt"
  else
    echo "worker-count=$workers FAILED TO DRAIN within 120s" | tee -a "$RAW_DIR/drain_throughput.txt"
  fi

  stop_app
done

echo "Done. Raw output in $RAW_DIR/"
