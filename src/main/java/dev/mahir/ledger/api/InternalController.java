package dev.mahir.ledger.api;

import dev.mahir.ledger.api.dto.OutboxStatsResponse;
import dev.mahir.ledger.store.OutboxRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/internal/outbox")
public class InternalController {

    private final OutboxRepository outboxRepository;

    public InternalController(OutboxRepository outboxRepository) {
        this.outboxRepository = outboxRepository;
    }

    @GetMapping("/stats")
    public OutboxStatsResponse stats() {
        Map<String, Long> counts = outboxRepository.countsByState();
        return new OutboxStatsResponse(counts.get("PENDING"), counts.get("PUBLISHED"), counts.get("DEAD"));
    }
}
