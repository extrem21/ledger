package dev.mahir.ledger.api;

import dev.mahir.ledger.support.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** T2, T3, T4 from LEDGER_SPEC.md §9. */
class OrderIdempotencyIT extends IntegrationTestBase {

    private static final String BODY = """
            {"customerId":"8f14e45f-ceea-467a-9e4d-3e2a1b7c9d01","currency":"INR",
             "items":[{"sku":"APL-USB-C-60W","quantity":2,"unitPriceCents":349000}]}
            """;

    private static final String DIFFERENT_BODY = """
            {"customerId":"8f14e45f-ceea-467a-9e4d-3e2a1b7c9d01","currency":"INR",
             "items":[{"sku":"APL-USB-C-60W","quantity":99,"unitPriceCents":349000}]}
            """;

    private ResponseEntity<String> post(String idempotencyKey, String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", idempotencyKey);
        headers.set("Content-Type", "application/json");
        return rest.postForEntity("/orders", new HttpEntity<>(body, headers), String.class);
    }

    @Test
    void t2_sequentialDuplicateSubmissions_createExactlyOneOrder() {
        String key = UUID.randomUUID().toString();

        ResponseEntity<String> first = post(key, BODY);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        Set<String> distinctBodies = new HashSet<>();
        distinctBodies.add(first.getBody());

        for (int i = 0; i < 99; i++) {
            ResponseEntity<String> retry = post(key, BODY);
            assertThat(retry.getStatusCode()).isEqualTo(HttpStatus.OK);
            distinctBodies.add(retry.getBody());
        }

        assertThat(distinctBodies).hasSize(1);
        Integer orderCount = jdbc.queryForObject("SELECT COUNT(*) FROM orders", Integer.class);
        assertThat(orderCount).isEqualTo(1);
    }

    @Test
    void t3_concurrentDuplicateSubmissions_createExactlyOneOrder() throws InterruptedException {
        String key = UUID.randomUUID().toString();
        int n = 50;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch ready = new CountDownLatch(n);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(n);
        Set<String> distinctBodies = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
        AtomicInteger successes = new AtomicInteger();

        for (int i = 0; i < n; i++) {
            pool.submit(() -> {
                try {
                    ready.countDown();
                    go.await();
                    ResponseEntity<String> resp = post(key, BODY);
                    if (resp.getStatusCode() == HttpStatus.CREATED || resp.getStatusCode() == HttpStatus.OK) {
                        successes.incrementAndGet();
                    }
                    distinctBodies.add(resp.getBody());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await();
        go.countDown();
        done.await(30, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(successes.get()).isEqualTo(n);
        assertThat(distinctBodies).hasSize(1);
        Integer orderCount = jdbc.queryForObject("SELECT COUNT(*) FROM orders", Integer.class);
        assertThat(orderCount).isEqualTo(1);
    }

    @Test
    void t4_sameKeyDifferentBody_returns409() {
        String key = UUID.randomUUID().toString();

        ResponseEntity<String> first = post(key, BODY);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ResponseEntity<String> second = post(key, DIFFERENT_BODY);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        Integer orderCount = jdbc.queryForObject("SELECT COUNT(*) FROM orders", Integer.class);
        assertThat(orderCount).isEqualTo(1);
    }

    @Test
    void missingIdempotencyKey_returns400() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        ResponseEntity<String> resp = rest.postForEntity("/orders", new HttpEntity<>(BODY, headers), String.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
