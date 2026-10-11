package dev.mahir.ledger.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.mahir.ledger.outbox.OrderSnapshotEvent;
import dev.mahir.ledger.store.OutboxEvent;
import dev.mahir.ledger.store.ProjectionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ProjectionConsumer {

    private static final String CONSUMER_NAME = "projection";

    private final ProjectionRepository projectionRepository;
    private final ObjectMapper objectMapper;

    public ProjectionConsumer(ProjectionRepository projectionRepository, ObjectMapper objectMapper) {
        this.projectionRepository = projectionRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * NESTED (savepoint-based) rather than the default REQUIRED: the caller
     * (InProcessEventPublisher, invoked from within OutboxDrainWorker's
     * batch transaction) needs a duplicate-key conflict here to roll back
     * only this event's work, not poison the whole batch. Plain Java
     * try/catch around the insert would NOT be enough — Postgres marks the
     * whole transaction aborted the instant a statement fails, regardless
     * of whether the exception is caught in application code.
     */
    @Transactional(propagation = Propagation.NESTED, rollbackFor = Exception.class)
    public void consume(OutboxEvent event) {
        // Throws DuplicateKeyException on a processed_events PK conflict —
        // that's the dedupe check, and the caller decides it's a no-op.
        projectionRepository.markProcessed(event.id(), CONSUMER_NAME);

        OrderSnapshotEvent snapshot = readSnapshot(event.payload());
        projectionRepository.upsertProjection(snapshot.orderId(), snapshot.status(), snapshot.totalCents(), event.id());
    }

    private OrderSnapshotEvent readSnapshot(String payloadJson) {
        try {
            return objectMapper.readValue(payloadJson, OrderSnapshotEvent.class);
        } catch (Exception e) {
            throw new IllegalStateException("malformed outbox payload: " + payloadJson, e);
        }
    }
}
