package dev.mahir.ledger.store;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class ProjectionRepository {

    private final JdbcTemplate jdbc;

    public ProjectionRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Throws org.springframework.dao.DuplicateKeyException on a primary-key
     * conflict — that IS the dedupe check. The caller decides what "already
     * processed" means for it; this method never swallows the conflict.
     */
    public void markProcessed(long eventId, String consumer) {
        jdbc.update("INSERT INTO processed_events (event_id, consumer) VALUES (?, ?)", eventId, consumer);
    }

    public void upsertProjection(UUID orderId, String status, long totalCents, long lastEventId) {
        jdbc.update("""
                INSERT INTO order_projection (order_id, status, total_cents, last_event_id, last_updated_at)
                VALUES (?, ?, ?, ?, now())
                ON CONFLICT (order_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    total_cents = EXCLUDED.total_cents,
                    last_event_id = EXCLUDED.last_event_id,
                    last_updated_at = now()
                """,
                orderId, status, totalCents, lastEventId);
    }

    public Optional<ProjectionRow> findByOrderId(UUID orderId) {
        List<ProjectionRow> rows = jdbc.query("""
                SELECT order_id, status, total_cents, last_event_id, last_updated_at
                FROM order_projection WHERE order_id = ?
                """,
                (rs, n) -> new ProjectionRow(
                        (UUID) rs.getObject("order_id"),
                        rs.getString("status"),
                        rs.getLong("total_cents"),
                        rs.getLong("last_event_id"),
                        rs.getTimestamp("last_updated_at").toInstant()),
                orderId);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }
}
