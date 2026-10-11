package dev.mahir.ledger.outbox;

import dev.mahir.ledger.config.OutboxProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * min(base * 2^attempts, cap) with +/-20% jitter, so a batch of failed
 * events doesn't retry in lockstep and hammer the sink in synchronised waves.
 */
@Component
public class BackoffPolicy {

    private static final double JITTER_FRACTION = 0.2;

    private final OutboxProperties properties;

    public BackoffPolicy(OutboxProperties properties) {
        this.properties = properties;
    }

    public Duration delayFor(int attempts) {
        long base = properties.getBackoffBaseMs();
        long cap = properties.getBackoffCapMs();
        double exponential = base * Math.pow(2, attempts);
        long capped = (long) Math.min(cap, exponential);

        double jitterFactor = 1.0 + (ThreadLocalRandom.current().nextDouble(-JITTER_FRACTION, JITTER_FRACTION));
        long withJitter = Math.round(capped * jitterFactor);
        return Duration.ofMillis(Math.max(0, withJitter));
    }
}
