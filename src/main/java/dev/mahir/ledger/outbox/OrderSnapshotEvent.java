package dev.mahir.ledger.outbox;

import java.time.Instant;
import java.util.UUID;

/**
 * The outbox payload shape for both OrderCreated and OrderStatusChanged
 * events — deliberately just what ProjectionConsumer needs to upsert
 * order_projection, not a full aggregate snapshot.
 */
public record OrderSnapshotEvent(UUID orderId, String status, long totalCents, Instant occurredAt) {
}
