package dev.mahir.ledger.domain;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record Order(
        UUID id,
        UUID customerId,
        OrderStatus status,
        long totalCents,
        String currency,
        int version,
        Instant createdAt,
        Instant updatedAt,
        List<OrderItem> items
) {
    public Order withStatus(OrderStatus newStatus) {
        return new Order(id, customerId, newStatus, totalCents, currency, version + 1,
                createdAt, updatedAt, items);
    }
}
