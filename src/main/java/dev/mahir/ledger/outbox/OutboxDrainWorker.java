package dev.mahir.ledger.outbox;

import dev.mahir.ledger.config.OutboxProperties;
import dev.mahir.ledger.store.OutboxEvent;
import dev.mahir.ledger.store.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Claims and publishes one batch of pending outbox events. Called through
 * the Spring proxy by OutboxScheduler (a different bean) — see CLAUDE.md §5
 * Trap 1: if the scheduling loop lived in this same class and called this
 * method on `this`, @Transactional would silently do nothing.
 */
@Component
public class OutboxDrainWorker {

    private static final Logger log = LoggerFactory.getLogger(OutboxDrainWorker.class);
    private static final int MAX_ERROR_LENGTH = 1000;

    private final OutboxRepository outboxRepository;
    private final EventPublisher eventPublisher;
    private final BackoffPolicy backoffPolicy;
    private final OutboxProperties properties;

    public OutboxDrainWorker(OutboxRepository outboxRepository, EventPublisher eventPublisher,
                              BackoffPolicy backoffPolicy, OutboxProperties properties) {
        this.outboxRepository = outboxRepository;
        this.eventPublisher = eventPublisher;
        this.backoffPolicy = backoffPolicy;
        this.properties = properties;
    }

    /**
     * FOR UPDATE (in OutboxRepository.claimBatch) row-locks every claimed
     * event until this transaction ends, so this whole method — claim,
     * publish attempts, and the resulting state updates — must stay inside
     * one transaction for SKIP LOCKED to mean anything.
     */
    @Transactional
    public int drainBatch() {
        List<OutboxEvent> batch = outboxRepository.claimBatch(properties.getBatchSize());
        for (OutboxEvent event : batch) {
            processOne(event);
        }
        return batch.size();
    }

    private void processOne(OutboxEvent event) {
        try {
            eventPublisher.publish(event);
            outboxRepository.markPublished(event.id());
        } catch (Exception e) {
            int attempts = event.attempts() + 1;
            String error = truncate(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            if (attempts >= properties.getMaxAttempts()) {
                outboxRepository.markDead(event.id(), attempts, error);
                log.warn("outbox event {} moved to DEAD after {} attempts: {}", event.id(), attempts, error);
            } else {
                Instant nextAttemptAt = Instant.now().plus(backoffPolicy.delayFor(attempts));
                outboxRepository.markFailed(event.id(), attempts, error, nextAttemptAt);
            }
        }
    }

    private String truncate(String message) {
        return message.length() > MAX_ERROR_LENGTH ? message.substring(0, MAX_ERROR_LENGTH) : message;
    }
}
