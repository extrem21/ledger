package dev.mahir.ledger.store;

import java.time.Instant;
import java.util.UUID;

public record ProjectionRow(
        UUID orderId,
        String status,
        long totalCents,
        long lastEventId,
        Instant lastUpdatedAt
) {
}
