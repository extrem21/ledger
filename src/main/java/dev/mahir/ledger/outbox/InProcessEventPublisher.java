package dev.mahir.ledger.outbox;

import dev.mahir.ledger.consumer.ProjectionConsumer;
import dev.mahir.ledger.store.OutboxEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "ledger.outbox.sink", havingValue = "in-process", matchIfMissing = true)
public class InProcessEventPublisher implements EventPublisher {

    private final ProjectionConsumer consumer;

    public InProcessEventPublisher(ProjectionConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void publish(OutboxEvent event) {
        try {
            consumer.consume(event);
        } catch (DuplicateKeyException alreadyProcessed) {
            // A previous delivery attempt already applied this event.
            // That's the dedupe working as intended, not a failure of
            // *this* delivery — let it count as published.
        }
    }
}
