package dev.mahir.ledger.api.dto;

import dev.mahir.ledger.domain.Order;
import dev.mahir.ledger.domain.OrderItem;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderDetailResponse(
        UUID orderId,
        UUID customerId,
        String status,
        long totalCents,
        String currency,
        List<Item> items,
        Instant createdAt,
        Instant updatedAt
) {
    public record Item(String sku, int quantity, long unitPriceCents) {
    }

    public static OrderDetailResponse from(Order order) {
        List<Item> items = order.items().stream()
                .map(i -> new Item(i.sku(), i.quantity(), i.unitPriceCents()))
                .toList();
        return new OrderDetailResponse(order.id(), order.customerId(), order.status().name(),
                order.totalCents(), order.currency(), items, order.createdAt(), order.updatedAt());
    }
}
