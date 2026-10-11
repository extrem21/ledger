package dev.mahir.ledger.domain;

import java.util.UUID;

public record OrderItem(
        Long id,
        UUID orderId,
        String sku,
        int quantity,
        long unitPriceCents
) {
    public long lineTotalCents() {
        return quantity * unitPriceCents;
    }
}
