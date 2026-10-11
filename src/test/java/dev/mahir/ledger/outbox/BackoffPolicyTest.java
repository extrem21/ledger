package dev.mahir.ledger.outbox;

import dev.mahir.ledger.config.OutboxProperties;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffPolicyTest {

    private final OutboxProperties properties = new OutboxProperties();
    private final BackoffPolicy policy = new BackoffPolicy(properties);

    @Test
    void growsExponentiallyWithinJitterBounds() {
        properties.setBackoffBaseMs(200);
        properties.setBackoffCapMs(30_000);

        for (int attempts = 1; attempts <= 5; attempts++) {
            long expected = (long) (200 * Math.pow(2, attempts));
            Duration delay = policy.delayFor(attempts);
            assertThat(delay.toMillis())
                    .isBetween((long) (expected * 0.79), (long) (expected * 1.21));
        }
    }

    @Test
    void neverExceedsCap() {
        properties.setBackoffBaseMs(200);
        properties.setBackoffCapMs(1_000);

        Duration delay = policy.delayFor(10);
        assertThat(delay.toMillis()).isLessThanOrEqualTo(1_200); // cap + jitter headroom
    }
}
