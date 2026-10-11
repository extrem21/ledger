# Ledger — Development Log

Running notes per build stage: what was built, measured test results, and
anything that didn't go the way the spec assumed it would. Numbers here are
taken directly from actual `mvn verify` runs — nothing rounded or guessed.

---

## Stage 1 — Write path and idempotency

**Scope:** Compose + Flyway migration, `POST /orders` / `GET /orders/{id}`,
outbox insert in the write transaction, idempotency keys, state machine +
transition endpoints, error handling. Corresponds to spec §12 milestones 0–5.

### What was built

- `V1__init.sql` — schema exactly as spec §4, no changes.
- `domain/` — `Order`, `OrderItem`, `OrderStatus`, `StateMachine` (explicit
  `Map<OrderStatus, Set<OrderStatus>>`, no Spring/JDBC imports).
- `store/` — `OrderRepository`, `OutboxRepository`, `IdempotencyRepository`,
  all `JdbcTemplate`, all SQL confined to this package.
- `api/` — `OrderController`, `OrderService` (the `@Transactional` entry
  point, called directly from the controller per CLAUDE.md §5 Trap 1),
  DTOs as records, `GlobalExceptionHandler`.
- Idempotency: insert-first, catch `DuplicateKeyException` on the
  `idempotency_keys` PK, re-read the winner. No check-then-insert anywhere.

### Test results — `mvn verify`, 2026-08-23

| Test class | Tests | Failures | Time |
|---|---|---|---|
| `StateMachineTest` (unit) | 13 | 0 | 0.087s |
| `OrderApiIT` (round-trip, 404, 422, 400) | 4 | 0 | 0.049s |
| `OrderIdempotencyIT` (T2, T3, T4 + missing-key) | 4 | 0 | 5.694s |
| `OrderTransitionIT` (T7, table-driven) | 13 | 0 | 0.412s |
| `OrderAtomicityIT` (T8) | 1 | 0 | 0.388s |
| **Total** | **35** | **0** | — |

Exit-criteria mapping (CLAUDE.md §6 Stage 1):

| Criterion | Result |
|---|---|
| T8 — atomicity: failure injected between order insert and outbox insert → zero rows in both | PASS |
| T2 — 100 sequential same-key POSTs → 1 order row, 100 identical response bodies | PASS |
| T3 — 50 concurrent same-key POSTs → 1 order row, 50 identical response bodies | PASS |
| T4 — same key, different body → `409`, order count unchanged | PASS |
| T7 — table-driven illegal transitions → `409`, no outbox row written | PASS (13 (state, action) pairs) |

### Challenges

1. **No local JDK 21 or Maven.** The host only had Corretto 11 and no Maven
   binary. Installed `openjdk@21` via Homebrew (`brew install openjdk@21`,
   keg-only at `/opt/homebrew/opt/openjdk@21`) rather than routing every
   build through a throwaway Maven Docker container — needed for `make run`
   / `make test` to work directly per the Makefile's intent.

2. **Testcontainers couldn't reach Docker Desktop.** With
   `testcontainers.version = 1.20.1` (the version `wrapper:wrapper`/initial
   scaffold pulled in), every container start failed:
   `BadRequestException (Status 400: {...})` on the `/info` probe, both from
   inside a Docker-outside-of-Docker container and running natively on the
   host. Raw `curl --unix-socket` against the same socket worked fine, which
   pointed at a docker-java version incompatibility with this Docker Desktop
   release rather than a socket/permissions problem. Fixed by bumping to
   `1.21.4` (latest 1.x; did not jump to the 2.x major to avoid an unrelated
   API migration mid-stage).

3. **Shared Testcontainers Postgres container was being stopped mid-suite.**
   `IntegrationTestBase` declared `POSTGRES` as a `static` field so every IT
   subclass would reuse one container. But with `@Testcontainers` +
   `@Container`, lifecycle is bound to each *test class's* before/after-all —
   even though the field is inherited, a later subclass's `afterAll` still
   stopped the one shared container object out from under Spring contexts
   that earlier subclasses had already built and cached (identical
   `@SpringBootTest` config → Spring reuses the context, including its
   Hikari pool pointing at the now-dead container's port). Symptom:
   `CannotGetJdbcConnection: Connection refused` on whatever port the
   original container had bound. Fixed by switching to the documented
   "singleton container" pattern — start the container once in a static
   initializer, never call `.stop()`, let Ryuk reap it at JVM exit.

4. **Real bug — idempotency replay returned the wrong HTTP status.** First
   cut of `OrderController` echoed the *stored* `response_status` column
   (always `201`, since that's what a creation stores) on the replay path
   instead of literally returning `200`. T2 caught this immediately
   (`expected: 200 OK but was: 201 CREATED`). Fixed by having the controller
   set the HTTP status explicitly per branch (`CREATED` for the request that
   wins the insert, `OK` for every replay) rather than trusting the stored
   value.

5. **Real bug — "identical responses" wasn't literally true.** T3 failed
   with 2 distinct response bodies out of 50 concurrent requests: the winner
   returned its in-memory Jackson serialization (`{"orderId":...}`, no
   spaces), while every retry returned the same JSON re-read from Postgres
   `JSONB` (`{"orderId": ...}` — Postgres's `jsonb::text` output inserts a
   space after each `:`). Same content, different bytes. Fixed by making
   *every* response — including the creator's — source its body from the
   freshly-inserted `idempotency_keys` row, so there's exactly one code path
   producing response bytes regardless of which branch ran.

---

## Stage 2 — Drain, retry, consume

**Scope:** Outbox drain worker (`FOR UPDATE SKIP LOCKED`), retry/backoff,
dead-letter, projection consumer with dedupe, `/orders/{id}/projection`,
`/internal/outbox/stats`. Corresponds to spec §12 milestones 6–8.

### What was built

- `config/OutboxProperties` — binds `ledger.outbox.*` (delay, batch size,
  max attempts, backoff base/cap, sink, webhook URL, worker count).
- `outbox/BackoffPolicy` — `min(base * 2^attempts, cap)` ± 20% jitter.
- `outbox/EventPublisher` — `InProcessEventPublisher` (default, calls the
  consumer directly) and `WebhookEventPublisher` (HTTP POST, used for T5),
  selected via `@ConditionalOnProperty` on `ledger.outbox.sink`.
- `outbox/OutboxDrainWorker` — `@Transactional` claim-batch-and-process,
  called through the proxy by a *separate* bean, `OutboxScheduler`, which
  runs `worker-count` independent `ScheduledExecutorService` loops (not
  Spring's `@Scheduled`, which only gives one execution stream — no way to
  get N concurrent ticks out of a single `@Scheduled` method).
- `consumer/ProjectionConsumer` — dedupe insert into `processed_events` +
  upsert into `order_projection`, `@Transactional(propagation = NESTED)`.
- `OrderService` now emits a shared minimal `OrderSnapshotEvent` payload
  (orderId, status, totalCents, occurredAt) for both `OrderCreated` and
  `OrderStatusChanged` events, replacing the earlier full-`Order` snapshot —
  needed once the consumer had to build/update `order_projection` from
  either event type without a type-specific branch.

### Test results — `mvn verify`, 2026-08-24

| Test class | Tests | Failures | Time |
|---|---|---|---|
| `BackoffPolicyTest` (unit) | 2 | 0 | 0.043s |
| `OutboxPoisonEventIT` (T5) | 1 | 0 | 6.801s |
| `OutboxConcurrentDrainIT` (T6, 10,000 events / 4 workers) | 1 | 0 | 5.998s |
| Stage 1 suite (re-run, unchanged) | 35 | 0 | — |
| **Total** | **39** | **0** | — |

Exit-criteria mapping (CLAUDE.md §6 Stage 2):

| Criterion | Result |
|---|---|
| T6 — 4 workers, 10,000 pending events → every event published exactly once, no duplicate `processed_events`, no leftovers | PASS |
| T5 — poison event reaches `DEAD` after exactly `MAX_ATTEMPTS`; backoff intervals grow; other events keep publishing throughout | PASS |

### Challenges

1. **Trap 1, in the drain loop this time.** The natural first draft put the
   polling loop and the `@Transactional` claim-and-process method in the
   same class, called via `this::drainBatch`. Caught before writing it, by
   applying the same fix CLAUDE.md prescribes for the controller/service
   split: `OutboxDrainWorker` (holds `@Transactional`) and `OutboxScheduler`
   (holds the executor, injects the worker bean, calls it from outside) are
   two separate beans, so the call always crosses the proxy.

2. **A caught exception doesn't undo a Postgres-level abort.** The consumer
   needs to skip silently on a `processed_events` PK conflict — but that
   insert runs inside the same physical transaction as the outer
   `drainBatch()` call (default `REQUIRED` propagation joins it). Catching
   the `DuplicateKeyException` in Java code does *not* recover the
   connection: Postgres marks the whole transaction aborted the instant a
   statement fails, and every later statement in it raises "current
   transaction is aborted" regardless of what application code catches.
   Fixed with `@Transactional(propagation = NESTED)` on
   `ProjectionConsumer.consume()`, which uses a real JDBC savepoint — a
   conflict there rolls back just that nested portion and leaves the outer
   batch transaction (and the rest of that batch's events) intact.

3. **Real test-isolation bug, not an application bug.** First T5/T6 run: T6
   failed with 9,291/10,000 published, the rest dying with "connection
   refused" against T5's stub webhook server — a port T5 had already closed.
   Cause: `IntegrationTestBase`'s singleton Postgres container is
   intentionally shared JVM-wide (see Stage 1 log), but Spring also caches
   *application contexts* by property set — so T5's context (webhook sink,
   background `OutboxScheduler` threads) stayed alive and kept polling the
   same shared `outbox` table after T5's test method returned, racing T6's
   freshly-seeded rows. Fixed with `@DirtiesContext(classMode = AFTER_CLASS)`
   on both `OutboxPoisonEventIT` and `OutboxConcurrentDrainIT`, so their
   contexts (and the `@PreDestroy`-stopped scheduler threads) are torn down
   before the next test class runs. First fix attempt silently no-op'd
   because I ran `failsafe:integration-test` directly after editing the test
   source without an intervening `test-compile` — worth remembering:
   `failsafe:integration-test` alone does not recompile.

4. **No new test dependency for the T5 stub.** Needed an HTTP endpoint that
   returns 500 for one specific "poison" order and 200 for everything else.
   Used the JDK's built-in `com.sun.net.httpserver.HttpServer` rather than
   pulling in WireMock or similar — avoids a dependency-addition ask for
   something the JDK already provides.

---

## Stage 3 — Prove it, then ship it

**Scope:** T1 crash harness, benchmarks, README, CI. Corresponds to spec
§12 milestones 9-11 (AWS deploy, milestone 12, addressed separately below).

### What was built

- `harness/t1_crash.sh` — standalone bash harness (not JUnit — it needs to
  `kill -9` a real OS process). Runs the app as a plain `java -jar` process
  against dockerized Postgres (not itself containerized, so `kill -9 $PID`
  is unambiguous), seeds 50 orders per trial via parallel `curl`, kills at
  a random point (0-600ms after seeding, with `batch-size` overridden to 10
  so a 50-order drain takes ~5 ticks instead of finishing in one), restarts,
  polls `/internal/outbox/stats` until settled, and asserts
  `COUNT(orders) == COUNT(outbox WHERE PUBLISHED) == COUNT(order_projection)`
  with zero `DEAD`. Postgres is *not* reset between trials — results
  accumulate, and the invariant holds globally regardless of how many
  trials have contributed rows, so there's no per-trial cleanup cost.
- `bench/write_throughput.js` — k6 script for `POST /orders`, supporting
  both max-throughput mode (`constant-vus`) and a fixed-rate mode
  (`constant-arrival-rate`, via `-e RATE=`) for lag measurement.
- `bench/run.sh` — orchestrates the full benchmark: warmup, write-throughput
  ramp (10/25/50/100 VUs), isolated end-to-end lag measurement, and
  drain-throughput at 1/2/4 workers (events pre-seeded directly into
  `outbox`, bypassing the write path, to isolate pure drain speed).
- `README.md` — diagram, dual-write explanation, design trade-offs, results
  table, benchmark headlines, quickstart, limitations.

### Results — measured, 2026-08-24

**T1 crash harness:** 50/50 trials reconciled, median recovery 1807ms
(`./harness/t1_crash.sh`, full log in that run's stdout). Recovery time is
measured from the moment the restarted process is launched to
`pending: 0` on `/internal/outbox/stats`, so it includes full JVM + Spring
Boot boot time, not just drain time — an honest "how long until the system
is healthy again" number, not a best-case drain-only figure.

**Full `mvn verify`:** 39 tests, 0 failures (unchanged from Stage 2, rerun
to confirm nothing regressed while building the harness/bench scripts).

**Benchmarks:** see `bench/results.md` for the full table. Headline: write
throughput plateaus at ~2600-2900 req/s across the whole 10-100 VU range
(0 failures throughout) while p50 latency grows from 3.3ms to 29.7ms —
consistent with HikariCP's default 10-connection pool saturating, not a
write-path bottleneck. Drain throughput scales 308 → 622 → 1,153 events/s
at 1 → 2 → 4 workers (near-linear, as `SKIP LOCKED` promises).

### Challenges

1. **Local Maven kept silently using the wrong JDK.** The T1 harness
   script exported `JAVA_HOME` with `${JAVA_HOME:-/opt/homebrew/...}` —
   which only applies the default when the variable is *unset*. The user's
   shell profile already exports `JAVA_HOME` pointing at the system
   Corretto 11 install, so the fallback silently never took effect, and
   `./mvnw package` failed with "compiled by a more recent version of the
   Java Runtime (class file version 61.0)... only recognizes up to 55.0" —
   a confusing error that's really just "wrong JDK." Fixed by
   unconditionally setting `JAVA_HOME` (and prepending it to `PATH`) in the
   harness/bench scripts rather than trying to respect an inherited value.

2. **First benchmark run produced nonsense end-to-end-lag numbers** — p50
   90.6s, p99 156.4s. Root cause: the lag query joined *every* order created
   during the entire write-throughput ramp (all four VU levels, 42,150
   orders), and at 50-100 VUs the single default drain worker (~300
   events/s ceiling, measured separately) can't keep up with write
   throughput — a backlog builds during the burst, and orders created near
   the end of it wait a long time before the worker even gets to them once
   the burst stops generating new load. That's real queueing delay behind
   an artificial stress test, not per-event pipeline latency. First fix
   attempt (a lighter `constant-vus` load at 5 VUs) still generated ~920
   orders/s on this hardware — still well over the ~300/s drain ceiling,
   same problem at smaller scale. Fixed by giving `write_throughput.js` a
   `constant-arrival-rate` mode (`-e RATE=`) and measuring lag against a
   genuinely rate-limited 20 req/s, 10s load (201 orders) — that produced
   the reported p50 62.6ms / p99 242.5ms, in the range the 100ms poll tick
   would predict.

### CI

`.github/workflows/ci.yml` — JDK 21, cached Maven, `mvn verify`, Docker
build. First draft included a `services: postgres` container with
`DB_HOST`/`DB_PORT` env vars — removed before it ever ran, because it was
dead weight: the integration tests use Testcontainers, which spins up its
*own* Postgres via the runner's Docker daemon (available by default on
`ubuntu-latest`) and wires the dynamic port itself through
`IntegrationTestBase`. The fixed service container would have sat there
unused. **Not yet verified against a real run** — this repo has no GitHub
remote configured in this environment, so "CI green on push" from the exit
criteria is unconfirmed; the workflow is written to match how the test
suite actually connects to Postgres, not run end-to-end.

### AWS deploy (milestone 12)

Not attempted this session — no AWS credentials available in this
environment. Per CLAUDE.md §6, this is the correct thing to drop first if
time is short ("a local project with proven crash safety beats a deployed
one without it"), and everything ahead of it (T1, benchmarks, README) is
done. Deploying is mechanical from here (EC2 `t3.micro` + RDS
`db.t4g.micro`, credentials via env vars) whenever AWS access is available.
