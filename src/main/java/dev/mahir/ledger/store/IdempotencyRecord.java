package dev.mahir.ledger.store;

import java.util.UUID;

public record IdempotencyRecord(
        String key,
        String requestFingerprint,
        UUID orderId,
        int responseStatus,
        String responseBody
) {
}
