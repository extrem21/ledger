package dev.mahir.ledger.outbox;

import dev.mahir.ledger.support.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T6 — 4 concurrent drain workers against 10,000
 * pending events. Proves FOR UPDATE SKIP LOCKED: every event published
 * exactly once, no duplicate processed_events, no leftovers.
 *
 * @DirtiesContext: prevents this context's 4 lingering worker threads from
 * outliving the test class and racing later tests' outbox rows.
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OutboxConcurrentDrainIT extends IntegrationTestBase {

    private static final int EVENT_COUNT = 10_000;

    @DynamicPropertySource
    static void workerCount(DynamicPropertyRegistry registry) {
        registry.add("ledger.outbox.worker-count", () -> 4);
        registry.add("ledger.outbox.batch-size", () -> 100);
    }

    @Test
    void tenThousandEvents_fourWorkers_allPublishedExactlyOnce() throws InterruptedException {
        seedPendingEvents(EVENT_COUNT);

        long deadline = System.currentTimeMillis() + 60_000;
        long pending;
        do {
            Thread.sleep(200);
            pending = jdbc.queryForObject("SELECT COUNT(*) FROM outbox WHERE state = 'PENDING'", Long.class);
        } while (pending > 0 && System.currentTimeMillis() < deadline);
        assertThat(pending).isZero();

        Long published = jdbc.queryForObject("SELECT COUNT(*) FROM outbox WHERE state = 'PUBLISHED'", Long.class);
        Long dead = jdbc.queryForObject("SELECT COUNT(*) FROM outbox WHERE state = 'DEAD'", Long.class);
        Long processed = jdbc.queryForObject("SELECT COUNT(*) FROM processed_events", Long.class);

        assertThat(published).isEqualTo(EVENT_COUNT);
        assertThat(dead).isZero();
        // processed_events PK is (event_id, consumer) — this count can only equal
        // EVENT_COUNT if every event was applied exactly once, never twice.
        assertThat(processed).isEqualTo(EVENT_COUNT);
    }

    private void seedPendingEvents(int count) {
        String sql = """
                INSERT INTO outbox (aggregate_id, event_type, payload)
                VALUES (?, 'OrderCreated', ?::jsonb)
                """;
        jdbc.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(java.sql.PreparedStatement ps, int i) throws java.sql.SQLException {
                UUID orderId = UUID.randomUUID();
                String payload = """
                        {"orderId":"%s","status":"PENDING","totalCents":100,"occurredAt":"%s"}
                        """.formatted(orderId, Instant.now());
                ps.setObject(1, orderId);
                ps.setString(2, payload);
            }

            @Override
            public int getBatchSize() {
                return count;
            }
        });
    }
}
