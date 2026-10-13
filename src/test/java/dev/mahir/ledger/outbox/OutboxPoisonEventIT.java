package dev.mahir.ledger.outbox;

import com.sun.net.httpserver.HttpServer;
import dev.mahir.ledger.support.IntegrationTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T5 — a poison event (webhook sink always returns
 * 500 for it) must reach DEAD after exactly MAX_ATTEMPTS, with growing
 * backoff, while other events keep publishing normally throughout.
 *
 * Uses the JDK's built-in com.sun.net.httpserver.HttpServer as the stub
 * webhook target — no new test dependency needed.
 *
 * @DirtiesContext: this context's OutboxScheduler points at a webhook URL
 * whose stub server dies with this class. Without tearing the context down,
 * its background workers keep polling the shared outbox table and fail any
 * later test's events against the now-closed port.
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OutboxPoisonEventIT extends IntegrationTestBase {

    private static final int MAX_ATTEMPTS = 3;
    private static final int SERVER_PORT = 18089;

    private static final UUID POISON_ORDER_ID = UUID.randomUUID();
    private static final List<Instant> poisonHitTimestamps = new CopyOnWriteArrayList<>();

    private HttpServer server;

    @DynamicPropertySource
    static void webhookProperties(DynamicPropertyRegistry registry) {
        registry.add("ledger.outbox.sink", () -> "webhook");
        registry.add("ledger.outbox.webhook-url", () -> "http://localhost:" + SERVER_PORT + "/webhook");
        registry.add("ledger.outbox.max-attempts", () -> MAX_ATTEMPTS);
        registry.add("ledger.outbox.backoff-base-ms", () -> 500);
        registry.add("ledger.outbox.backoff-cap-ms", () -> 10_000);
        registry.add("ledger.outbox.drain-fixed-delay-ms", () -> 50);
    }

    @BeforeEach
    void startStubServer() throws IOException {
        poisonHitTimestamps.clear();
        server = HttpServer.create(new InetSocketAddress(SERVER_PORT), 0);
        server.createContext("/webhook", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            int status;
            if (body.contains(POISON_ORDER_ID.toString())) {
                poisonHitTimestamps.add(Instant.now());
                status = 500;
            } else {
                status = 200;
            }
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopStubServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void poisonEvent_reachesDead_withoutBlockingOtherEvents() throws InterruptedException {
        insertEvent(POISON_ORDER_ID);
        List<UUID> healthyOrderIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            UUID id = UUID.randomUUID();
            healthyOrderIds.add(id);
            insertEvent(id);
        }

        // Healthy events should all publish quickly, well before the poison
        // event exhausts its retries.
        long deadline = System.currentTimeMillis() + 15_000;
        long healthyPending;
        do {
            Thread.sleep(100);
            healthyPending = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM outbox WHERE state = 'PENDING' AND aggregate_id <> ?",
                    Long.class, POISON_ORDER_ID);
        } while (healthyPending > 0 && System.currentTimeMillis() < deadline);
        assertThat(healthyPending).isZero();

        for (UUID id : healthyOrderIds) {
            String state = jdbc.queryForObject("SELECT state FROM outbox WHERE aggregate_id = ?", String.class, id);
            assertThat(state).isEqualTo("PUBLISHED");
        }

        // Now wait for the poison event to exhaust MAX_ATTEMPTS and go DEAD.
        deadline = System.currentTimeMillis() + 15_000;
        String poisonState;
        do {
            Thread.sleep(100);
            poisonState = jdbc.queryForObject("SELECT state FROM outbox WHERE aggregate_id = ?", String.class, POISON_ORDER_ID);
        } while (!"DEAD".equals(poisonState) && System.currentTimeMillis() < deadline);

        assertThat(poisonState).isEqualTo("DEAD");
        Integer attempts = jdbc.queryForObject("SELECT attempts FROM outbox WHERE aggregate_id = ?", Integer.class, POISON_ORDER_ID);
        assertThat(attempts).isEqualTo(MAX_ATTEMPTS);
        assertThat(poisonHitTimestamps).hasSize(MAX_ATTEMPTS);

        // Backoff should grow: each gap should not be dramatically smaller
        // than the previous one (loose bound tolerant of +/-20% jitter).
        for (int i = 2; i < poisonHitTimestamps.size(); i++) {
            long prevGapMs = java.time.Duration.between(poisonHitTimestamps.get(i - 2), poisonHitTimestamps.get(i - 1)).toMillis();
            long gapMs = java.time.Duration.between(poisonHitTimestamps.get(i - 1), poisonHitTimestamps.get(i)).toMillis();
            assertThat(gapMs).isGreaterThan((long) (prevGapMs * 1.2));
        }
    }

    private void insertEvent(UUID orderId) {
        String payload = """
                {"orderId":"%s","status":"PENDING","totalCents":100,"occurredAt":"%s"}
                """.formatted(orderId, Instant.now());
        jdbc.update("INSERT INTO outbox (aggregate_id, event_type, payload) VALUES (?, 'OrderCreated', ?::jsonb)",
                orderId, payload);
    }
}
