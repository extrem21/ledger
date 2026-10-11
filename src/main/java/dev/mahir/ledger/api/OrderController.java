package dev.mahir.ledger.api;

import dev.mahir.ledger.api.dto.OrderDetailResponse;
import dev.mahir.ledger.api.dto.ProjectionResponse;
import dev.mahir.ledger.api.exception.IdempotencyConflictException;
import dev.mahir.ledger.api.exception.MissingIdempotencyKeyException;
import dev.mahir.ledger.domain.OrderStatus;
import dev.mahir.ledger.store.IdempotencyRecord;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<String> create(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody String rawBody) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new MissingIdempotencyKeyException();
        }

        HttpStatus status;
        try {
            orderService.createOrder(idempotencyKey, rawBody);
            status = HttpStatus.CREATED;
        } catch (DuplicateKeyException e) {
            status = HttpStatus.OK;
        }

        // Always return the response bytes actually persisted in idempotency_keys —
        // for both the creator and every retry — so "identical responses" (T3) holds
        // literally, not just semantically.
        IdempotencyRecord record = orderService.loadIdempotencyRecord(idempotencyKey);
        if (status == HttpStatus.OK && !record.requestFingerprint().equals(OrderService.fingerprint(rawBody))) {
            throw new IdempotencyConflictException(idempotencyKey);
        }
        return ResponseEntity.status(status)
                .contentType(MediaType.APPLICATION_JSON)
                .body(record.responseBody());
    }

    @GetMapping("/{id}")
    public OrderDetailResponse get(@PathVariable UUID id) {
        return orderService.getOrder(id);
    }

    @GetMapping("/{id}/projection")
    public ProjectionResponse projection(@PathVariable UUID id) {
        return orderService.getProjection(id);
    }

    @PostMapping("/{id}/confirm")
    public OrderDetailResponse confirm(@PathVariable UUID id) {
        orderService.transition(id, OrderStatus.CONFIRMED);
        return orderService.getOrder(id);
    }

    @PostMapping("/{id}/ship")
    public OrderDetailResponse ship(@PathVariable UUID id) {
        orderService.transition(id, OrderStatus.SHIPPED);
        return orderService.getOrder(id);
    }

    @PostMapping("/{id}/cancel")
    public OrderDetailResponse cancel(@PathVariable UUID id) {
        orderService.transition(id, OrderStatus.CANCELLED);
        return orderService.getOrder(id);
    }
}
