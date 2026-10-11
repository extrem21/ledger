package dev.mahir.ledger.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.mahir.ledger.api.dto.CreateOrderRequest;
import dev.mahir.ledger.api.dto.OrderDetailResponse;
import dev.mahir.ledger.api.dto.OrderResponse;
import dev.mahir.ledger.api.exception.IllegalTransitionException;
import dev.mahir.ledger.api.exception.MalformedRequestException;
import dev.mahir.ledger.api.exception.OrderNotFoundException;
import dev.mahir.ledger.api.exception.ValidationException;
import dev.mahir.ledger.domain.Order;
import dev.mahir.ledger.domain.OrderItem;
import dev.mahir.ledger.domain.OrderStatus;
import dev.mahir.ledger.domain.StateMachine;
import dev.mahir.ledger.api.dto.ProjectionResponse;
import dev.mahir.ledger.outbox.OrderSnapshotEvent;
import dev.mahir.ledger.store.IdempotencyRecord;
import dev.mahir.ledger.store.IdempotencyRepository;
import dev.mahir.ledger.store.OrderRepository;
import dev.mahir.ledger.store.OrderStatusAndTotal;
import dev.mahir.ledger.store.OutboxRepository;
import dev.mahir.ledger.store.ProjectionRepository;
import dev.mahir.ledger.store.ProjectionRow;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * The transactional entry point for the write path. Every public method here
 * is called directly from OrderController (a different bean), so Spring's
 * @Transactional proxy is always in play — see CLAUDE.md §5 Trap 1.
 */
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final IdempotencyRepository idempotencyRepository;
    private final ProjectionRepository projectionRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository, OutboxRepository outboxRepository,
                         IdempotencyRepository idempotencyRepository, ProjectionRepository projectionRepository,
                         ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.idempotencyRepository = idempotencyRepository;
        this.projectionRepository = projectionRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional(rollbackFor = Exception.class)
    public void createOrder(String idempotencyKey, String rawBody) {
        CreateOrderRequest req = parseCreateRequest(rawBody);
        validate(req);

        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();
        long totalCents = req.items().stream()
                .mapToLong(i -> i.quantity() * i.unitPriceCents())
                .sum();

        List<OrderItem> items = req.items().stream()
                .map(i -> new OrderItem(null, orderId, i.sku(), i.quantity(), i.unitPriceCents()))
                .toList();

        Order order = new Order(orderId, req.customerId(), OrderStatus.PENDING, totalCents,
                req.currency(), 0, now, now, items);

        orderRepository.insert(order);
        OrderSnapshotEvent snapshot = new OrderSnapshotEvent(orderId, order.status().name(), order.totalCents(), now);
        outboxRepository.insert(orderId, "OrderCreated", writeJson(snapshot));

        OrderResponse response = OrderResponse.from(order);
        String responseJson = writeJson(response);
        // Throws DuplicateKeyException on a PK conflict — the caller (OrderController)
        // catches it and resolves the winner. This is the sole arbiter; there is
        // deliberately no SELECT-before-INSERT here.
        idempotencyRepository.insert(idempotencyKey, fingerprint(rawBody), orderId, 201, responseJson);
    }

    public IdempotencyRecord loadIdempotencyRecord(String key) {
        return idempotencyRepository.findByKey(key)
                .orElseThrow(() -> new IllegalStateException(
                        "idempotency key vanished after a duplicate-key conflict: " + key));
    }

    public OrderDetailResponse getOrder(UUID id) {
        Order order = orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
        return OrderDetailResponse.from(order);
    }

    @Transactional(rollbackFor = Exception.class)
    public void transition(UUID orderId, OrderStatus target) {
        OrderStatusAndTotal current = orderRepository.findStatusForUpdate(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        if (!StateMachine.isLegal(current.status(), target)) {
            throw new IllegalTransitionException(current.status(), target);
        }
        orderRepository.updateStatus(orderId, target);
        OrderSnapshotEvent snapshot = new OrderSnapshotEvent(orderId, target.name(), current.totalCents(), Instant.now());
        outboxRepository.insert(orderId, "OrderStatusChanged", writeJson(snapshot));
    }

    public ProjectionResponse getProjection(UUID orderId) {
        ProjectionRow row = projectionRepository.findByOrderId(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        return new ProjectionResponse(row.orderId(), row.status(), row.totalCents(), row.lastEventId(), row.lastUpdatedAt());
    }

    private CreateOrderRequest parseCreateRequest(String rawBody) {
        try {
            return objectMapper.readValue(rawBody, CreateOrderRequest.class);
        } catch (Exception e) {
            throw new MalformedRequestException("Malformed request body: " + e.getMessage(), e);
        }
    }

    private void validate(CreateOrderRequest req) {
        if (req.customerId() == null) {
            throw new ValidationException("customerId is required");
        }
        if (req.currency() == null || req.currency().length() != 3) {
            throw new ValidationException("currency must be a 3-letter code");
        }
        if (req.items() == null || req.items().isEmpty()) {
            throw new ValidationException("items must not be empty");
        }
        for (CreateOrderRequest.OrderItemRequest item : req.items()) {
            if (item.sku() == null || item.sku().isBlank()) {
                throw new ValidationException("item sku is required");
            }
            if (item.quantity() == null || item.quantity() <= 0) {
                throw new ValidationException("item quantity must be positive");
            }
            if (item.unitPriceCents() == null || item.unitPriceCents() < 0) {
                throw new ValidationException("item unitPriceCents must not be negative");
            }
        }
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialize " + value.getClass(), e);
        }
    }

    static String fingerprint(String rawBody) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(rawBody.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
