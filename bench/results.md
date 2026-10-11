# Benchmark results

Run 2026-08-24, local machine — Apple M2, 8 cores, 8 GB RAM, macOS 26.2,
Docker 29.6.1 (Docker Desktop). Postgres 16 in Docker; app run as a plain
`java -jar` process on the host talking to that container over
`localhost:5432` (not containerized itself, so it isn't competing with
Postgres for the same cgroup). Command: `./bench/run.sh` (raw output under
`bench/raw/`).

All numbers below came out of that run — none are estimated or rounded up.

## Methodology

- 30s warmup against the write path, discarded (JIT warmup — early requests
  measure JIT compilation, not the service).
- Write throughput: `k6 run -e VUS=<n> -e DURATION=20s bench/write_throughput.js`
  for VUs = 10, 25, 50, 100, each a fresh `POST /orders` with a unique
  `Idempotency-Key` (never hitting the idempotency replay path).
- End-to-end lag: measured as its own **separate, rate-limited** pass
  (`k6 run -e RATE=20 -e DURATION=10s bench/write_throughput.js`, ~200
  orders over 10s), not read off the throughput-ramp data. At 50-100 VUs
  the single default drain worker (batch-size 50, 100ms tick, ~300 events/s
  measured ceiling — see drain throughput below) can't keep up with write
  throughput, so a backlog builds; lag measured over that data would mostly
  be queueing delay behind an artificial burst, not per-event pipeline
  latency. A rate well under drain capacity isolates the latter. Lag itself
  is computed from Postgres's own timestamps
  (`order_projection.last_updated_at - orders.created_at`), avoiding any
  host/container clock-skew concern.
- Drain throughput: `ledger.outbox.worker-count` set to 1, 2, 4 in turn;
  20,000 events pre-seeded directly into `outbox` (bypassing the write path
  to isolate pure drain speed), timed from seed to
  `GET /internal/outbox/stats` reporting `pending: 0`.

## Write throughput — `POST /orders`

| VUs | req/s | p50 | p95 | p99 | max | errors |
|---|---|---|---|---|---|---|
| 10 | 2648.2/s | 3.27ms | 5.92ms | 9.33ms | 163.93ms | 0/52975 |
| 25 | 2938.1/s | 7.04ms | 16.37ms | 24.93ms | 162.99ms | 0/58781 |
| 50 | 2629.4/s | 14.46ms | 41.81ms | 71.04ms | 817.41ms | 0/52631 |
| 100 | 2715.9/s | 29.69ms | 90.10ms | 144.52ms | 333.10ms | 0/54399 |

**Highest sustainable rate: ~2900 req/s at 25 VUs.** Throughput plateaus in
a tight band (~2600-2900 req/s) across the whole 10→100 VU range while p50
latency grows roughly linearly with VUs (3.3ms → 29.7ms) — that shape (flat
throughput, growing queueing latency) is the signature of a saturated
resource, most likely HikariCP's default 10-connection pool serializing
requests once concurrency exceeds it, not the write logic itself. Zero
request failures at every level — the system degrades via latency, not
errors.

## End-to-end lag — commit → projection applied

Fixed-rate load (20 req/s, 10s, 201 orders), single default drain worker,
100ms tick:

| Metric | Value |
|---|---|
| p50 | 62.6ms |
| p99 | 242.5ms |
| n | 201 orders |

Consistent with a 100ms polling tick: an order committed just after a tick
waits nearly a full cycle before being claimed; p50 landing near half the
tick interval plus consumer processing is the expected shape. p99 reflects
occasional scheduling/GC jitter.

## Drain throughput — `SKIP LOCKED` scaling

20,000 pre-seeded pending events per run:

| Workers | Events | Elapsed | Rate |
|---|---|---|---|
| 1 | 20,000 | 64.88s | 308/s |
| 2 | 20,000 | 32.14s | 622/s |
| 4 | 20,000 | 17.34s | 1153/s |

Near-linear scaling (1.00x → 2.02x → 3.74x) — this is the payoff of
`FOR UPDATE SKIP LOCKED`: each additional worker claims a disjoint batch
instead of blocking behind the others.

## Reproduce

```
./bench/run.sh
```

Builds the jar, starts a fresh Postgres, runs all of the above in sequence,
and writes raw k6/psql output to `bench/raw/`.
