package dev.mahir.ledger.store;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class IdempotencyRepository {

    private final JdbcTemplate jdbc;

    public IdempotencyRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Throws org.springframework.dao.DuplicateKeyException on a primary-key
     * conflict — that is the arbiter for concurrent/duplicate submissions,
     * not a SELECT performed beforehand.
     */
    public void insert(String key, String requestFingerprint, UUID orderId, int responseStatus, String responseBodyJson) {
        jdbc.update("""
                INSERT INTO idempotency_keys (key, request_fingerprint, order_id, response_status, response_body)
                VALUES (?, ?, ?, ?, ?::jsonb)
                """,
                key, requestFingerprint, orderId, responseStatus, responseBodyJson);
    }

    public Optional<IdempotencyRecord> findByKey(String key) {
        List<IdempotencyRecord> rows = jdbc.query("""
                SELECT key, request_fingerprint, order_id, response_status, response_body::text AS response_body
                FROM idempotency_keys WHERE key = ?
                """,
                (rs, n) -> new IdempotencyRecord(
                        rs.getString("key"),
                        rs.getString("request_fingerprint"),
                        (UUID) rs.getObject("order_id"),
                        rs.getInt("response_status"),
                        rs.getString("response_body")),
                key);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }
}
