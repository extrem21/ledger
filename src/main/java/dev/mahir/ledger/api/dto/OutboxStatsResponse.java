package dev.mahir.ledger.api.dto;

public record OutboxStatsResponse(long pending, long published, long dead) {
}
