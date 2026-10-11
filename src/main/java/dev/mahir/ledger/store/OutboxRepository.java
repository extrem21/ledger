package dev.mahir.ledger.store;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class OutboxRepository {

    private static final List<String> STATES = List.of("PENDING", "PUBLISHED", "DEAD");

    private final JdbcTemplate jdbc;

    public OutboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(UUID aggregateId, String eventType, String payloadJson) {
        jdbc.update("""
                INSERT INTO outbox (aggregate_id, event_type, payload)
                VALUES (?, ?, ?::jsonb)
                """,
                aggregateId, eventType, payloadJson);
    }

    /**
     * Row-locks each claimed event until the caller's transaction ends.
     * SKIP LOCKED lets other concurrent callers step over rows this one
     * already holds, instead of blocking behind them — that's the entire
     * concurrency story for the drain worker.
     */
    public List<OutboxEvent> claimBatch(int batchSize) {
        return jdbc.query("""
                SELECT id, aggregate_id, event_type, payload::text AS payload_text, attempts
                FROM outbox
                WHERE state = 'PENDING' AND next_attempt_at <= now()
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT ?
                """,
                (rs, n) -> new OutboxEvent(
                        rs.getLong("id"),
                        (UUID) rs.getObject("aggregate_id"),
                        rs.getString("event_type"),
                        rs.getString("payload_text"),
                        rs.getInt("attempts")),
                batchSize);
    }

    public void markPublished(long id) {
        jdbc.update("UPDATE outbox SET state = 'PUBLISHED', published_at = now() WHERE id = ?", id);
    }

    public void markFailed(long id, int attempts, String lastError, Instant nextAttemptAt) {
        jdbc.update("UPDATE outbox SET attempts = ?, last_error = ?, next_attempt_at = ? WHERE id = ?",
                attempts, lastError, Timestamp.from(nextAttemptAt), id);
    }

    public void markDead(long id, int attempts, String lastError) {
        jdbc.update("UPDATE outbox SET state = 'DEAD', attempts = ?, last_error = ? WHERE id = ?",
                attempts, lastError, id);
    }

    public Map<String, Long> countsByState() {
        Map<String, Long> counts = new HashMap<>();
        STATES.forEach(s -> counts.put(s, 0L));
        jdbc.query("SELECT state, COUNT(*) AS cnt FROM outbox GROUP BY state",
                (RowCallbackHandler) rs -> counts.put(rs.getString("state"), rs.getLong("cnt")));
        return counts;
    }
}
