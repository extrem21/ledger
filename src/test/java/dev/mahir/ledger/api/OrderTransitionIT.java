package dev.mahir.ledger.api;

import dev.mahir.ledger.support.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** T7 from LEDGER_SPEC.md §9 — table-driven illegal transitions. */
class OrderTransitionIT extends IntegrationTestBase {

    private static final String BODY = """
            {"customerId":"8f14e45f-ceea-467a-9e4d-3e2a1b7c9d01","currency":"INR",
             "items":[{"sku":"APL-USB-C-60W","quantity":1,"unitPriceCents":100}]}
            """;

    static Stream<String> illegalActionsFromPending() {
        return Stream.of("ship");
    }

    static Stream<String> illegalActionsFromConfirmed() {
        return Stream.of("confirm");
    }

    static Stream<String> illegalActionsFromShipped() {
        return Stream.of("confirm", "ship", "cancel");
    }

    static Stream<String> illegalActionsFromCancelled() {
        return Stream.of("confirm", "ship", "cancel");
    }

    private UUID createOrder() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", UUID.randomUUID().toString());
        headers.set("Content-Type", "application/json");
        ResponseEntity<String> resp = rest.postForEntity("/orders", new HttpEntity<>(BODY, headers), String.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return jdbc.queryForObject("SELECT id FROM orders ORDER BY created_at DESC LIMIT 1", UUID.class);
    }

    private ResponseEntity<String> act(UUID id, String action) {
        return rest.postForEntity("/orders/" + id + "/" + action, HttpEntity.EMPTY, String.class);
    }

    private void assertIllegal(UUID id, String action) {
        long outboxBefore = countOutbox();
        ResponseEntity<String> resp = act(id, action);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(countOutbox()).isEqualTo(outboxBefore);
    }

    private long countOutbox() {
        return jdbc.queryForObject("SELECT COUNT(*) FROM outbox", Long.class);
    }

    @ParameterizedTest
    @MethodSource("illegalActionsFromPending")
    void pending_illegalActions_rejected(String action) {
        UUID id = createOrder();
        assertIllegal(id, action);
    }

    @ParameterizedTest
    @MethodSource("illegalActionsFromConfirmed")
    void confirmed_illegalActions_rejected(String action) {
        UUID id = createOrder();
        assertThat(act(id, "confirm").getStatusCode()).isEqualTo(HttpStatus.OK);
        assertIllegal(id, action);
    }

    @ParameterizedTest
    @MethodSource("illegalActionsFromShipped")
    void shipped_illegalActions_rejected(String action) {
        UUID id = createOrder();
        assertThat(act(id, "confirm").getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(act(id, "ship").getStatusCode()).isEqualTo(HttpStatus.OK);
        assertIllegal(id, action);
    }

    @ParameterizedTest
    @MethodSource("illegalActionsFromCancelled")
    void cancelled_illegalActions_rejected(String action) {
        UUID id = createOrder();
        assertThat(act(id, "cancel").getStatusCode()).isEqualTo(HttpStatus.OK);
        assertIllegal(id, action);
    }

    @ParameterizedTest
    @MethodSource("legalTransitions")
    void legalTransitions_succeedAndWriteOutboxRow(List<String> actions) {
        UUID id = createOrder();
        long before = countOutbox();
        for (String action : actions) {
            assertThat(act(id, action).getStatusCode()).isEqualTo(HttpStatus.OK);
        }
        assertThat(countOutbox()).isEqualTo(before + actions.size());
    }

    static Stream<List<String>> legalTransitions() {
        return Stream.of(
                List.of("confirm"),
                List.of("cancel"),
                List.of("confirm", "ship"),
                List.of("confirm", "cancel")
        );
    }

    @Test
    void unknownOrder_returns404() {
        ResponseEntity<String> resp = act(UUID.randomUUID(), "confirm");
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
