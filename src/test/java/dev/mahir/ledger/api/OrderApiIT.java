package dev.mahir.ledger.api;

import dev.mahir.ledger.support.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OrderApiIT extends IntegrationTestBase {

    private static final String BODY = """
            {"customerId":"8f14e45f-ceea-467a-9e4d-3e2a1b7c9d01","currency":"INR",
             "items":[{"sku":"APL-USB-C-60W","quantity":2,"unitPriceCents":349000}]}
            """;

    @Test
    void createThenGet_roundTrips() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", UUID.randomUUID().toString());
        headers.set("Content-Type", "application/json");

        ResponseEntity<String> created = rest.postForEntity("/orders", new HttpEntity<>(BODY, headers), String.class);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.getBody()).contains("PENDING").contains("698000");

        UUID id = jdbc.queryForObject("SELECT id FROM orders LIMIT 1", UUID.class);

        ResponseEntity<String> fetched = rest.getForEntity("/orders/" + id, String.class);
        assertThat(fetched.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(fetched.getBody()).contains("APL-USB-C-60W").contains("\"quantity\":2");
    }

    @Test
    void getUnknownOrder_returns404() {
        ResponseEntity<String> resp = rest.getForEntity("/orders/" + UUID.randomUUID(), String.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void negativeQuantity_returns422() {
        String badBody = """
                {"customerId":"8f14e45f-ceea-467a-9e4d-3e2a1b7c9d01","currency":"INR",
                 "items":[{"sku":"X","quantity":-1,"unitPriceCents":100}]}
                """;
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", UUID.randomUUID().toString());
        headers.set("Content-Type", "application/json");

        ResponseEntity<String> resp = rest.postForEntity("/orders", new HttpEntity<>(badBody, headers), String.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
    }

    @Test
    void malformedJson_returns400() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", UUID.randomUUID().toString());
        headers.set("Content-Type", "application/json");

        ResponseEntity<String> resp = rest.postForEntity("/orders", new HttpEntity<>("not json", headers), String.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
