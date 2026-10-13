package dev.mahir.ledger.api;

import dev.mahir.ledger.store.OutboxRepository;
import dev.mahir.ledger.support.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T8 — inject a failure between the order insert and
 * the outbox insert and assert zero rows land in either table. This is the
 * direct test for two Spring traps: self-invocation bypassing @Transactional,
 * and checked exceptions committing instead of rolling back.
 */
@Import(OrderAtomicityIT.FailingOutboxConfig.class)
class OrderAtomicityIT extends IntegrationTestBase {

    private static final String BODY = """
            {"customerId":"8f14e45f-ceea-467a-9e4d-3e2a1b7c9d01","currency":"INR",
             "items":[{"sku":"APL-USB-C-60W","quantity":1,"unitPriceCents":100}]}
            """;

    @Test
    void failureBeforeCommit_leavesNoPartialState() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", UUID.randomUUID().toString());
        headers.set("Content-Type", "application/json");

        ResponseEntity<String> resp = rest.postForEntity("/orders", new HttpEntity<>(BODY, headers), String.class);

        assertThat(resp.getStatusCode().is5xxServerError()).isTrue();
        assertThat(count("orders")).isZero();
        assertThat(count("order_items")).isZero();
        assertThat(count("outbox")).isZero();
        assertThat(count("idempotency_keys")).isZero();
    }

    private long count(String table) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
    }

    @TestConfiguration
    static class FailingOutboxConfig {
        @Bean
        @Primary
        OutboxRepository failingOutboxRepository(JdbcTemplate jdbc) {
            return new OutboxRepository(jdbc) {
                @Override
                public void insert(UUID aggregateId, String eventType, String payloadJson) {
                    throw new RuntimeException("T8 injected failure: simulated crash before outbox insert");
                }
            };
        }
    }
}
