package dev.mahir.ledger.store;

import dev.mahir.ledger.domain.OrderStatus;

public record OrderStatusAndTotal(OrderStatus status, long totalCents) {
}
