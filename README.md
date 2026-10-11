# Ledger — Idempotent Order Service

A Spring Boot order service that accepts orders over HTTP and emits exactly
one downstream effect per accepted order, with two guarantees that survive
`kill -9` and client retries:

1. **A retried order submission never creates a second order** — enforced
   by a client-supplied `Idempotency-Key`, arbitrated by a database
   `PRIMARY KEY`, not an application-level check.
2. **Every committed order produces exactly one downstream effect** — via a
   transactional outbox and an idempotent consumer.

The feature set is deliberately small. The value is that both guarantees
are proven under injected failure — a real crash harness, real concurrent
load — not just asserted in this README.

---

## How it works

```
POST /orders (Idempotency-Key: ...)
        │
        ▼
┌───────────────────────────────────────┐
│ ONE transaction (OrderService)         │
│  1. INSERT orders                      │
│  2. INSERT order_items                 │
│  3. INSERT outbox      (state=PENDING) │
│  4. INSERT idempotency_keys            │
│  commit — all four land, or none do    │
└───────────────────┬─────────────────────┘
                     │ commit
                     ▼
        outbox table (PENDING rows)
                     │
                     │ polled every 100ms, N concurrent workers
                     ▼
┌──────────────────────────────────────────────┐
│ OutboxDrainWorker                              │
│ SELECT ... WHERE state='PENDING'               │
│   FOR UPDATE SKIP LOCKED LIMIT batch_size       │
│                                                  │
│ per claimed event:                              │
│   success → state=PUBLISHED                     │
│   failure → attempts++, backoff, retry           │
│             (or DEAD at MAX_ATTEMPTS)             │
└───────────────────────┬─────────────────────────┘
                         │ publish()
                         ▼
              EventPublisher (in-process)
                         │
                         ▼
┌──────────────────────────────────────────────┐
│ ProjectionConsumer (NESTED transaction)        │
│  1. INSERT processed_events (dedupe)            │
│     — PK conflict → skip silently, already done │
│  2. UPSERT order_projection                      │
└──────────────────────────────────────────────┘
```

State transitions (`/confirm`, `/ship`, `/cancel`) follow the same shape:
validate against an explicit `PENDING → CONFIRMED → SHIPPED` / `CANCELLED`
state machine, update the order, write an outbox event — one transaction.
An illegal transition returns `409` and writes nothing.

## The dual-write problem, and why an outbox fixes it

After committing an order, the service has to tell the rest of the system
it happened. The naive approach — write to the database, then separately
publish an event — has no safe ordering, because a crash can land between
the two steps no matter which comes first:

- **Commit, then crash before publish:** the order exists, nobody
  downstream ever finds out. A silently lost event.
- **Publish, then crash before commit** (or the transaction rolls back):
  downstream reacts to an order that was never actually created. A phantom
  event for something that doesn't exist.

There's no way to sequence two independent systems (a database and a
message broker, or even a database and an HTTP call) to eliminate this —
the crash window exists between *any* two steps, however you order them.

The fix is to stop treating it as two systems. The outbox event is written
to a table **inside the same database transaction** as the order itself
(see the diagram above — it's insert #3, sitting between the order and its
own idempotency record). Either both the order and its event land, or
neither does — verified directly by T8, which injects a failure between
the order insert and the outbox insert and asserts zero rows land in
either table. A separate worker drains the outbox after the fact, and that
publish step can now fail and retry freely, because the event it's
retrying is durably recorded — retrying doesn't risk creating or losing
anything.

This gets you **at-least-once delivery**, not exactly-once — a crash
mid-publish means the worker can't know whether the downstream system saw
the event, so it retries, and may publish twice. The last piece is making
the *consumer* idempotent: it records every event ID it has applied
(`processed_events`) and skips anything it's already seen, in the same
transaction as the effect it applies. Delivery stays at-least-once; the
*observed effect* is exactly-once.

## Design decisions and their costs

**`JdbcTemplate` over JPA/Hibernate.** This project's whole point is
precise transactional control and a hand-written `FOR UPDATE SKIP LOCKED`
query — an ORM's job is specifically to hide the SQL that matters most
here. Cost: every query is hand-written, and there's no automatic
entity-relationship mapping, lazy loading, or change tracking — more
boilerplate for a bigger domain than this one.

**Postgres outbox over Kafka/RabbitMQ/SQS.** The outbox drain *is* the
queue; adding a broker means operating a second stateful system for a
workload one Postgres table handles comfortably at this scale (thousands
of events/sec — see benchmarks below). Cost: no built-in multi-consumer
fan-out, no long-term replay log, and throughput is bounded by what a
polling table scan can serve. The threshold to reconsider: sustained event
volume that outgrows a single table's polling throughput, multiple
independent consumer services needing their own delivery cursors, or a
hard requirement to replay history beyond what's still in the table.

**In-process consumer over a separate service.** `ProjectionConsumer` runs
in the same JVM as the drain worker — the "publish" step for the default
sink *is* the call into the consumer. Cost: the consumer can't be scaled,
deployed, or fail independently of the write/drain service; a slow
consumer directly slows the drain loop. The `EventPublisher` interface
exists specifically so a real, independent downstream (an HTTP webhook, or
eventually a broker) can be substituted per event type without touching
the outbox or write path.

## Results

All numbers below are measured, not estimated — see `DEVLOG.md` for the
full narrative including what went wrong along the way.

| Test | What it proves | Result |
|---|---|---|
| **T1 — crash harness** | Kill -9 mid-drain, restart, reconcile | **50/50 trials, median recovery 1807ms** |
| T2 — sequential duplicate submission | 100 sequential same-key POSTs → 1 order, 100 identical responses | PASS |
| T3 — concurrent duplicate submission | 50 simultaneous same-key POSTs → 1 order, 50 identical responses | PASS |
| T4 — idempotency key conflict | Same key, different body → `409` | PASS |
| T5 — poison event | Reaches `DEAD` after exactly `MAX_ATTEMPTS`; other events keep publishing | PASS |
| T6 — concurrent drain | 4 workers, 10,000 events → each published exactly once, no leftovers | PASS |
| T7 — illegal transitions | Table-driven, every (state, action) not in the legal set → `409`, no outbox row | PASS (13 cases) |
| T8 — atomicity | Failure injected between order insert and outbox insert → zero rows in both | PASS |

`mvn verify`: 39 tests, 0 failures (unit + Testcontainers integration tests
against real Postgres — T2, T3, T4, T5, T6, T7, T8, plus round-trip and
validation coverage). T1 is a standalone harness
(`harness/t1_crash.sh`) — it kills a real running process, which a JUnit
test can't do.

## Benchmarks

Full methodology, raw numbers, and hardware in
[`bench/results.md`](bench/results.md). Headlines from that run:

- **Write throughput:** ~2600-2900 req/s sustained across 10-100 concurrent
  clients, zero request failures at any level. Throughput plateaus while
  p50 latency grows with concurrency (3.3ms → 29.7ms) — the signature of a
  saturated resource (most likely HikariCP's default 10-connection pool),
  not the write logic itself.
- **Drain throughput:** 308 → 622 → 1,153 events/s at 1 → 2 → 4 workers —
  near-linear scaling, which is what `FOR UPDATE SKIP LOCKED` buys.
- **End-to-end lag** (commit → projection applied), isolated at a light
  steady rate: p50 62.6ms, p99 242.5ms — consistent with the 100ms drain
  poll interval.

## Quickstart

```
docker compose up -d --build
curl -X POST localhost:8080/orders \
  -H "Idempotency-Key: $(uuidgen)" -H "Content-Type: application/json" \
  -d '{"customerId":"8f14e45f-ceea-467a-9e4d-3e2a1b7c9d01","currency":"INR","items":[{"sku":"X","quantity":1,"unitPriceCents":500}]}'
```

Or via `make`: `make up`, `make test`, `make test-crash`, `make bench`,
`make psql`. See the Makefile for the full list.

## What this doesn't do

- **Single database, no partitioning or sharding.** The outbox table is a
  single point of write contention; this is fine at the scale measured
  above and would need rethinking well before Kafka-scale event volume.
- **No multi-region.** One Postgres instance, one availability story. A
  region failure takes the whole service down with it.
- **In-process consumer only.** The projection consumer can't scale or
  deploy independently of the drain worker (see design decisions above).
- **No event schema versioning.** The outbox payload shape
  (`OrderSnapshotEvent`) is whatever the current code produces; there's no
  mechanism for a consumer to handle an older payload shape from before a
  deploy.
- **No authentication or authorization.** Every endpoint is open — this is
  a demonstration of transactional correctness, not a production-ready
  API surface.
- **Default connection pool untuned.** The write-throughput plateau above
  is very likely HikariCP's default pool size, not investigated or tuned
  further here.
