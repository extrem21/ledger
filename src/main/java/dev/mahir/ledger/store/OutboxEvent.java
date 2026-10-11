package dev.mahir.ledger.store;

import java.util.UUID;

public record OutboxEvent(
        long id,
        UUID aggregateId,
        String eventType,
        String payload,
        int attempts
) {
}
