# Ledger — Idempotent Order Service

**This document is a complete, self-contained build specification.** It assumes no prior context. Anyone reading it — human or model — should be able to build the project from an empty directory.

---

## 1. What this is

A backend service that accepts customer orders over HTTP and reliably emits an event for every accepted order, with two correctness guarantees that survive process crashes and client retries:

1. **A retried order submission never creates a second order.**
2. **Every committed order produces exactly one downstream effect — no lost events, no duplicates.**

It is deliberately small. The interesting part is not the feature set; it is that the guarantees are *proven under injected failure*, not asserted.

### The two problems being solved

**Problem A — duplicate submission.** A client sends `POST /orders`, the network times out, the client retries. The first request may have already succeeded. Without protection, the customer is charged twice. Solution: a client-supplied **idempotency key** — a unique token per logical operation. The server records the key with the response it produced; a retry with the same key returns the stored response instead of re-executing.

**Problem B — the dual-write.** After committing an order to the database, the service must tell the rest of the system. The naive approach writes to the DB and then publishes an event as two separate operations. These can half-fail:

- Crash after DB commit, before publish → order exists, nobody knows. Lost event.
- Publish before commit, then rollback → downstream reacts to an order that does not exist. Phantom event.

There is no ordering of two independent systems that fixes this, because a crash can land between any two steps. Solution: the **transactional outbox**. The event is written to a table *in the same database transaction as the order*. Either both land or neither does. A separate worker later reads the outbox and publishes. The publish step can now be retried freely, because the event is durably recorded.

The outbox gives **at-least-once** delivery: after a crash mid-publish, the worker cannot know whether the publish succeeded, so it retries and may publish twice. This is converted to **exactly-once effect** by making consumers idempotent — each consumer records the event IDs it has processed and ignores repeats. "Exactly-once effect" is the honest term: delivery is at-least-once, but the observable result is as if it happened once.

---

## 2. Stack

| Component | Choice | Notes |
|---|---|---|
| Language | Java 21 | LTS |
| Framework | Spring Boot 3.3.x | Web + JDBC starters only |
| DB access | **`JdbcTemplate`** | **Not JPA/Hibernate** — see below |
| Database | PostgreSQL 16 | |
| Migrations | Flyway | Plain SQL files |
| Build | Maven | |
| Container | Docker + Docker Compose | |
| Load testing | k6 (or any HTTP load tool) | |
| CI/CD | GitHub Actions | |
| Deploy | AWS (EC2 + RDS, or ECS Fargate) | |

### Why JdbcTemplate and not JPA

JPA/Hibernate is an object-relational mapper: it hides SQL behind entity objects and manages a persistence context. It also introduces lazy loading, detached-entity errors, N+1 query problems, and cascade semantics — a large body of framework-specific behaviour that is expensive to learn and irrelevant to this project's goals.

`JdbcTemplate` is a thin wrapper over JDBC: you write the SQL, it manages connections and maps result rows. For a project whose entire point is precise transactional control and a hand-written `FOR UPDATE SKIP LOCKED` query, hiding the SQL is a liability.

**This is a defensible engineering decision, not a shortcut.** State it that way if asked.

### Why no Kafka

The outbox drain *is* the queue. Adding Kafka would mean operating a second stateful system for a workload Postgres handles comfortably at this scale. Choosing not to add infrastructure is a legitimate design decision — say so directly, and name the threshold at which it would change (roughly: when event volume, multi-consumer fan-out, or replay requirements exceed what a table scan can serve).

---

## 3. Domain model and state machine

An **order** has a status and moves through a fixed lifecycle:

```
PENDING ──confirm──▶ CONFIRMED ──ship──▶ SHIPPED
   │                     │
   └──────cancel─────────┘
              │
              ▼
          CANCELLED
```

Legal transitions — everything else is rejected with `409 Conflict`:

| From | Event | To |
|---|---|---|
| `PENDING` | confirm | `CONFIRMED` |
| `PENDING` | cancel | `CANCELLED` |
| `CONFIRMED` | ship | `SHIPPED` |
| `CONFIRMED` | cancel | `CANCELLED` |
| `SHIPPED` | — | terminal |
| `CANCELLED` | — | terminal |

Encode this as an explicit map in code (e.g. `Map<Status, Set<Status>>`), not as scattered `if` statements. It should be readable at a glance and unit-testable in isolation.

Every successful transition writes an event row to the outbox **in the same transaction** as the status update.

---

## 4. Database schema

`src/main/resources/db/migration/V1__init.sql`:

```sql
CREATE TABLE orders (
    id              UUID PRIMARY KEY,
    customer_id     UUID        NOT NULL,
    status          TEXT        NOT NULL
                    CHECK (status IN ('PENDING','CONFIRMED','SHIPPED','CANCELLED')),
    total_cents     BIGINT      NOT NULL CHECK (total_cents >= 0),
    currency        CHAR(3)     NOT NULL DEFAULT 'INR',
    version         INT         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    id                BIGSERIAL PRIMARY KEY,
    order_id          UUID   NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    sku               TEXT   NOT NULL,
    quantity          INT    NOT NULL CHECK (quantity > 0),
    unit_price_cents  BIGINT NOT NULL CHECK (unit_price_cents >= 0)
);
CREATE INDEX idx_order_items_order ON order_items(order_id);

-- Idempotency: one row per client-supplied key.
-- request_fingerprint is a hash of the request body, used to detect
-- the same key being reused for a DIFFERENT request (a client bug).
CREATE TABLE idempotency_keys (
    key                  TEXT PRIMARY KEY,
    request_fingerprint  TEXT        NOT NULL,
    order_id             UUID        NOT NULL REFERENCES orders(id),
    response_status      INT         NOT NULL,
    response_body        JSONB       NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The transactional outbox.
CREATE TABLE outbox (
    id               BIGSERIAL PRIMARY KEY,
    aggregate_id     UUID        NOT NULL,
    event_type       TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    state            TEXT        NOT NULL DEFAULT 'PENDING'
                     CHECK (state IN ('PENDING','PUBLISHED','DEAD')),
    attempts         INT         NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at     TIMESTAMPTZ
);

-- Partial index: the drain query only ever looks at PENDING rows.
CREATE INDEX idx_outbox_claimable
    ON outbox (next_attempt_at, id)
    WHERE state = 'PENDING';

-- Consumer-side dedupe. Composite key: the same event may be
-- consumed by several independent consumers.
CREATE TABLE processed_events (
    event_id      BIGINT      NOT NULL,
    consumer      TEXT        NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer)
);

-- Read model built by the consumer — proves events are actually applied.
CREATE TABLE order_projection (
    order_id           UUID PRIMARY KEY,
    status             TEXT        NOT NULL,
    total_cents        BIGINT      NOT NULL,
    last_event_id      BIGINT      NOT NULL,
    last_updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## 5. HTTP API

### `POST /orders`

Headers: `Idempotency-Key: <client-generated UUID>` (required; `400` if absent)

Request:
```json
{
  "customerId": "8f14e45f-ceea-467a-9e4d-3e2a1b7c9d01",
  "currency": "INR",
  "items": [
    { "sku": "APL-USB-C-60W", "quantity": 2, "unitPriceCents": 349000 }
  ]
}
```

Response `201`:
```json
{
  "orderId": "0a1b2c3d-4e5f-6789-abcd-ef0123456789",
  "status": "PENDING",
  "totalCents": 698000,
  "currency": "INR",
  "createdAt": "2026-08-23T10:04:12Z"
}
```

Behaviour:

| Condition | Result |
|---|---|
| New key | Create order + outbox event + idempotency record, all in one transaction. `201`. |
| Key seen, same body fingerprint | Return the **stored** response verbatim. `200`. Do not re-execute. |
| Key seen, different body fingerprint | `409 Conflict` — the client reused a key for a different request. |
| Concurrent duplicate keys | The `PRIMARY KEY` on `idempotency_keys` makes one insert fail. Catch the unique-violation, re-read the winner's stored response, return it. |

That last row matters: two simultaneous retries race. The database constraint is the arbiter, not application logic. Do not attempt a check-then-insert — that is a TOCTOU race.

### Other endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/orders/{id}` | Fetch order + items |
| `POST` | `/orders/{id}/confirm` | `PENDING → CONFIRMED` |
| `POST` | `/orders/{id}/ship` | `CONFIRMED → SHIPPED` |
| `POST` | `/orders/{id}/cancel` | `PENDING`/`CONFIRMED → CANCELLED` |
| `GET` | `/orders/{id}/projection` | Read model — proves the consumer applied events |
| `GET` | `/actuator/health` | Liveness |
| `GET` | `/internal/outbox/stats` | Counts by state — used by the test harness |

Errors: `400` malformed, `404` unknown order, `409` illegal transition or key conflict, `422` validation failure.

---

## 6. The write path

`POST /orders` executes exactly one database transaction containing:

1. `INSERT INTO orders`
2. `INSERT INTO order_items` (batch)
3. `INSERT INTO outbox` — `event_type = 'OrderCreated'`, payload = the order snapshot
4. `INSERT INTO idempotency_keys` — key, fingerprint, order id, and the serialised response

All four commit together or none do. **This single property is the core of the project.** If a crash occurs at any point before commit, the client sees a failure and retries; nothing partial persists. If it occurs after commit, the order, its event, and its idempotency record all exist.

State transitions (`/confirm`, `/ship`, `/cancel`) follow the same shape: validate the transition against the state machine, `UPDATE orders`, `INSERT INTO outbox` — one transaction.

---

## 7. The outbox drain worker

A scheduled loop (start with a fixed delay of 100 ms) that claims and publishes pending events.

### Claim query

```sql
SELECT id, aggregate_id, event_type, payload, attempts
FROM outbox
WHERE state = 'PENDING'
  AND next_attempt_at <= now()
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT :batchSize;
```

**`FOR UPDATE SKIP LOCKED` explained.** `FOR UPDATE` row-locks each returned row until the transaction ends. `SKIP LOCKED` tells Postgres: if a row is already locked by another transaction, do not wait for it — skip past and take the next one. Without `SKIP LOCKED`, a second worker blocks behind the first, serialising all throughput. With it, N workers claim disjoint batches and scale nearly linearly. This one clause is the entire concurrency story.

`ORDER BY id` keeps publication order stable, which makes reasoning about the read model easier.

### Per-event handling

For each claimed event:

- **Success** → `UPDATE outbox SET state='PUBLISHED', published_at=now() WHERE id=?`
- **Failure** → increment `attempts`, record `last_error`, set `next_attempt_at = now() + backoff(attempts)`, leave `state='PENDING'`
- **`attempts >= MAX_ATTEMPTS`** (default 5) → `state='DEAD'`

Backoff: `min(BASE * 2^attempts, CAP)` with jitter — e.g. base 200 ms, cap 30 s. Jitter (a random factor, say ±20%) prevents many failed events from retrying in lockstep and hammering the downstream in synchronised waves.

**Dead-letter state.** A permanently failing event (malformed payload, downstream rejecting it forever) must not block the queue. Marking it `DEAD` removes it from the claim query while preserving it for inspection. Expose the dead count on `/internal/outbox/stats`.

### The consumer

Keep it in-process. For each event, in one transaction:

1. `INSERT INTO processed_events (event_id, consumer) VALUES (?, ?)` — if this violates the primary key, the event was already processed: **skip silently**. This is the idempotency check.
2. Apply the effect: upsert `order_projection`.

This is what converts at-least-once delivery into exactly-once effect. The dedupe insert and the effect share a transaction, so they cannot diverge.

Optionally add an HTTP webhook sink behind a config flag, so the "publish" target is pluggable. Useful for the poison-event test — point it at an endpoint that always returns `500`.

---

## 8. Spring Boot specifics — two traps that fail silently

Both of these produce **no error message**. They silently break the guarantee this project exists to demonstrate. Understand them before writing code; they are also the two most likely interview questions about the project.

### Trap 1 — `@Transactional` self-invocation

Spring implements `@Transactional` with a **proxy**: it wraps your bean in a generated object that opens a transaction, delegates to the real method, then commits. The annotation only takes effect when the call arrives *through the proxy* — that is, from outside the class.

```java
@Service
public class OrderService {
    public Order create(Req r) {
        return doCreate(r);   // ✗ internal call — bypasses the proxy entirely
    }

    @Transactional
    Order doCreate(Req r) { ... }   // annotation does NOTHING here
}
```

The order insert and the outbox insert land in **separate autocommit transactions**. The dual-write bug is back, silently, in the exact code written to prevent it.

**Fix:** put `@Transactional` on the method called from outside (the entry point), or split into two beans so the call crosses a proxy boundary.

### Trap 2 — rollback only on unchecked exceptions

By default Spring rolls back on `RuntimeException` and `Error`. A **checked** exception commits the transaction.

```java
@Transactional
public void confirm(UUID id) throws PaymentDeclinedException {  // checked
    updateStatus(id, CONFIRMED);
    throw new PaymentDeclinedException();   // ✗ transaction COMMITS
}
```

**Fix:** use unchecked exceptions for failures that must roll back, or declare `@Transactional(rollbackFor = Exception.class)`.

### Verify both

Write a test that asserts the order row and outbox row share a transaction: force a failure after the order insert and assert **zero** rows in both tables. If either trap is present, this test fails — which is precisely why it is worth writing.

---

## 9. Failure tests — the part that matters

**Budget half your total build time for this section.** Without it the project is an ordinary CRUD service; with it, it is evidence you understand distributed failure. Every number on the resume comes from here.

Script each test so it runs reproducibly (`make test-crash`, etc.) and prints a pass/fail line.

### T1 — Crash between commit and drain

The headline test.

1. Seed N orders (start with 50 trials).
2. Start the drain worker.
3. `kill -9` the process at a random point during draining.
4. Restart. Wait for the queue to settle.
5. Assert: `COUNT(outbox WHERE state='PUBLISHED') == COUNT(orders)`, and every `order_projection` row has been applied **exactly once** (`processed_events` has no duplicates by construction, so assert row counts match).
6. Record time from restart to full reconciliation.

Report: trials passed / trials run, and median recovery time.

`kill -9` specifically — `SIGKILL` cannot be caught, so no shutdown hook runs. Anything less tests graceful shutdown, which is not the interesting case.

### T2 — Duplicate submission, same key

Fire the same `POST /orders` with an identical `Idempotency-Key` 100 times, sequentially. Assert exactly one row in `orders` and every response body identical.

### T3 — Concurrent duplicate submission

Same key, 50 requests fired **simultaneously**. Assert one order row and 50 identical responses. This exercises the unique-violation-catch path, which sequential retries never reach.

### T4 — Key reuse with a different body

Same key, different items. Assert `409`.

### T5 — Poison event

Point the sink at an endpoint returning `500`. Assert the event reaches `state='DEAD'` after exactly `MAX_ATTEMPTS`, that backoff intervals grow as expected, and — critically — that **other events continue publishing normally** throughout. A blocked queue head is the failure mode being ruled out.

### T6 — Concurrent drain workers

Run 4 workers against 10,000 pending events. Assert every event is published exactly once (no duplicate `processed_events`, no unpublished leftovers). This is what `SKIP LOCKED` buys.

### T7 — Illegal transitions

Table-driven: for each (state, action) pair not in the legal set, assert `409` and assert no outbox row was written.

### T8 — Transaction atomicity

Inject a failure between the order insert and the outbox insert. Assert both tables have zero new rows. Directly tests for the two Spring traps.

---

## 10. Benchmarking

Report numbers honestly. Methodology matters more than magnitude — a modest number with a described method beats a large unexplained one.

**Setup:** service and Postgres in Docker on one host. Record CPU, RAM, and whether the DB is local or RDS. Warm up for 30 s and discard those results (JVM JIT compilation makes early requests much slower — without a warm-up your p99 measures the compiler, not the service).

**Measure two things separately:**

1. **Write throughput** — sustained `POST /orders` requests/sec, with p50/p95/p99 latency. Ramp concurrency (10, 25, 50, 100) until latency degrades; report the highest sustainable rate.
2. **Drain throughput** — events published/sec at 1, 2, 4 workers. Shows the `SKIP LOCKED` scaling.

Also report **end-to-end lag**: time from order commit to projection update, p50 and p99.

Record the exact command used so the run is reproducible. Put the raw output in `bench/results.md`.

---

## 11. Repository layout

```
ledger/
├── README.md
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── pom.xml
├── src/main/java/dev/<you>/ledger/
│   ├── LedgerApplication.java
│   ├── api/            OrderController, request/response DTOs, error handler
│   ├── domain/         Order, OrderItem, OrderStatus, StateMachine
│   ├── store/          OrderRepository, OutboxRepository, IdempotencyRepository (JdbcTemplate)
│   ├── outbox/         OutboxDrainWorker, BackoffPolicy, EventPublisher
│   ├── consumer/       ProjectionConsumer
│   └── config/         DataSource, scheduling, properties
├── src/main/resources/
│   ├── application.yml
│   └── db/migration/V1__init.sql
├── src/test/java/...   unit + integration tests
├── harness/            crash-test scripts (T1–T8)
├── bench/              k6 scripts + results.md
└── .github/workflows/ci.yml
```

---

## 12. Build order

Work in this sequence. Each milestone is independently verifiable — do not proceed until the current one demonstrably works.

| # | Milestone | Done when |
|---|---|---|
| 0 | Spring warm-up on a throwaway CRUD service | You can explain DI and `@Transactional` proxying without notes |
| 1 | Compose up: Postgres + Flyway migration | Tables exist |
| 2 | `POST /orders` + `GET /orders/{id}`, no idempotency | Order round-trips |
| 3 | Outbox insert in the same transaction | T8 passes |
| 4 | Idempotency keys | T2, T3, T4 pass |
| 5 | State machine + transition endpoints | T7 passes |
| 6 | Drain worker with `SKIP LOCKED` | Events publish; T6 passes |
| 7 | Retries, backoff, dead-letter | T5 passes |
| 8 | Projection consumer with dedupe | Read model updates |
| 9 | **Crash harness (T1)** | 50 trials, all reconcile |
| 10 | Benchmarks | `bench/results.md` populated |
| 11 | GitHub Actions CI | Build + tests green on push |
| 12 | AWS deploy | Public health endpoint responds |

Milestone 9 is the one that must not be cut. If time runs short, drop AWS before dropping the crash harness — a locally-run project with proven crash safety is worth more than a deployed one without it.

---

## 13. CI and deployment

**GitHub Actions** (`.github/workflows/ci.yml`): on push — set up JDK 21, cache Maven, run `mvn verify` against a Postgres service container, build the Docker image. Optionally push to ECR and redeploy on `main`.

**AWS, cheapest path:** EC2 `t3.micro` running the container + RDS `db.t4g.micro` Postgres. Keep the security group tight and put credentials in environment variables, never in the repo. If you prefer managed: ECS Fargate + RDS.

Deployment is milestone 12 for a reason — it demonstrates operational competence but adds nothing to the correctness story.

---

## 14. README requirements for the finished repo

The repo README should contain, in order:

1. One paragraph: what it is and the two guarantees.
2. **A diagram** of the write path and drain loop.
3. The dual-write problem and why the outbox solves it — in plain prose. This is the section a reviewer actually reads.
4. Design decisions and their trade-offs: JdbcTemplate over JPA, Postgres over Kafka, in-process consumer over a separate service. State what each costs.
5. **A results table**: every failure test, trials run, trials passed, recovery time.
6. Benchmark numbers with the methodology and hardware stated.
7. `docker compose up` quickstart.
8. An honest "what this does not do" section — no multi-region, no partitioning, single-database, in-process consumer. Naming limitations is a credibility signal, not a weakness.

---

## 15. Resume bullets this produces

Replace every bracketed figure with a measured value. Do not round upward.

> **Ledger** — Idempotent Order Service | Java, Spring Boot, PostgreSQL, Docker, AWS
>
> - Built an order service over a PENDING → CONFIRMED → SHIPPED state machine, rejecting illegal transitions and collapsing retried submissions via client-supplied idempotency keys so a replayed request never creates a second order.
> - Eliminated the dual-write failure mode by committing each order row and its outbound event in a single transaction and draining a transactional outbox asynchronously, so no order exists without an event and no event without an order.
> - Converted the outbox's at-least-once delivery into exactly-once effect with idempotent consumers, exponential-backoff retries and a dead-letter state, so a permanently failing event never blocks the drain.
> - Verified crash safety by killing the process between commit and drain across [N/N] trials — every event published exactly once, median recovery [N] ms — and sustained [N] orders/sec at [N] ms p50 / [N] ms p99. Deployed on AWS behind a GitHub Actions pipeline.

---

## 16. Questions to be able to answer

If any answer is not immediate, that part is not yet understood well enough to claim.

1. Why can't you just write to the DB and then publish the event?
2. What exactly does `FOR UPDATE SKIP LOCKED` do, and what breaks without `SKIP LOCKED`?
3. Your outbox delivers at-least-once. How is the *effect* exactly-once?
4. What happens if two clients submit with the same idempotency key at the same instant?
5. Why doesn't a permanently failing event block every event behind it?
6. Why JdbcTemplate instead of JPA?
7. Why not Kafka? At what point would you add it?
8. How does `@Transactional` actually work, and when does it silently do nothing?
9. What does this design not handle?
