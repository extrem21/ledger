package dev.mahir.ledger.api.dto;

import java.util.List;
import java.util.UUID;

public record CreateOrderRequest(
        UUID customerId,
        String currency,
        List<OrderItemRequest> items
) {
    public record OrderItemRequest(String sku, Integer quantity, Long unitPriceCents) {
    }
}
