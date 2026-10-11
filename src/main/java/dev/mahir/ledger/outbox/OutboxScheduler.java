package dev.mahir.ledger.outbox;

import dev.mahir.ledger.config.OutboxProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs `worker-count` independent, concurrently-ticking drain loops so
 * FOR UPDATE SKIP LOCKED actually has concurrency to demonstrate (T6, and
 * the 1/2/4-worker drain benchmark). Deliberately not Spring's @Scheduled:
 * a single @Scheduled method gives one execution stream (fixedDelay serialises
 * runs), not N concurrent ones.
 */
@Component
public class OutboxScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxScheduler.class);

    private final OutboxDrainWorker drainWorker;
    private final OutboxProperties properties;
    private ScheduledExecutorService executor;

    public OutboxScheduler(OutboxDrainWorker drainWorker, OutboxProperties properties) {
        this.drainWorker = drainWorker;
        this.properties = properties;
    }

    @PostConstruct
    void start() {
        int workerCount = properties.getWorkerCount();
        executor = Executors.newScheduledThreadPool(workerCount, r -> {
            Thread t = new Thread(r, "outbox-drain");
            t.setDaemon(true);
            return t;
        });
        for (int i = 0; i < workerCount; i++) {
            executor.scheduleWithFixedDelay(this::tick, 0, properties.getDrainFixedDelayMs(), TimeUnit.MILLISECONDS);
        }
    }

    private void tick() {
        try {
            drainWorker.drainBatch();
        } catch (Exception e) {
            log.error("outbox drain tick failed", e);
        }
    }

    @PreDestroy
    void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
