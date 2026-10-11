package dev.mahir.ledger.api.dto;

import dev.mahir.ledger.domain.Order;

import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
        UUID orderId,
        String status,
        long totalCents,
        String currency,
        Instant createdAt
) {
    public static OrderResponse from(Order order) {
        return new OrderResponse(order.id(), order.status().name(), order.totalCents(),
                order.currency(), order.createdAt());
    }
}
