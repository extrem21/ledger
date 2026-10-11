package dev.mahir.ledger.outbox;

import dev.mahir.ledger.store.OutboxEvent;

/**
 * The publish step. In-process by default (invokes the consumer directly);
 * a webhook sink is available behind config for pointing failures at a
 * real HTTP endpoint (T5). Implementations throw an unchecked exception on
 * failure — OutboxDrainWorker treats any exception here as "not delivered."
 */
public interface EventPublisher {
    void publish(OutboxEvent event);
}
