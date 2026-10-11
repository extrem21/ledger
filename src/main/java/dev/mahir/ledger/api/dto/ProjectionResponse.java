package dev.mahir.ledger.api.dto;

import java.time.Instant;
import java.util.UUID;

public record ProjectionResponse(
        UUID orderId,
        String status,
        long totalCents,
        long lastEventId,
        Instant lastUpdatedAt
) {
}
