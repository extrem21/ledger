package dev.mahir.ledger.store;

import dev.mahir.ledger.domain.Order;
import dev.mahir.ledger.domain.OrderItem;
import dev.mahir.ledger.domain.OrderStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class OrderRepository {

    private final JdbcTemplate jdbc;

    public OrderRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(Order order) {
        jdbc.update("""
                INSERT INTO orders (id, customer_id, status, total_cents, currency, version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                order.id(), order.customerId(), order.status().name(), order.totalCents(),
                order.currency(), order.version(), Timestamp.from(order.createdAt()),
                Timestamp.from(order.updatedAt()));

        List<OrderItem> items = order.items();
        jdbc.batchUpdate("""
                INSERT INTO order_items (order_id, sku, quantity, unit_price_cents)
                VALUES (?, ?, ?, ?)
                """,
                items,
                items.size(),
                (ps, item) -> {
                    ps.setObject(1, order.id());
                    ps.setString(2, item.sku());
                    ps.setInt(3, item.quantity());
                    ps.setLong(4, item.unitPriceCents());
                });
    }

    public Optional<Order> findById(UUID id) {
        List<Order> orders = jdbc.query(
                "SELECT id, customer_id, status, total_cents, currency, version, created_at, updated_at FROM orders WHERE id = ?",
                ORDER_ROW_MAPPER, id);
        if (orders.isEmpty()) {
            return Optional.empty();
        }
        Order order = orders.get(0);
        List<OrderItem> items = jdbc.query(
                "SELECT id, order_id, sku, quantity, unit_price_cents FROM order_items WHERE order_id = ? ORDER BY id",
                ITEM_ROW_MAPPER, id);
        return Optional.of(new Order(order.id(), order.customerId(), order.status(), order.totalCents(),
                order.currency(), order.version(), order.createdAt(), order.updatedAt(), items));
    }

    /** Row-locks the order for the duration of the caller's transaction. */
    public Optional<OrderStatusAndTotal> findStatusForUpdate(UUID id) {
        List<OrderStatusAndTotal> rows = jdbc.query(
                "SELECT status, total_cents FROM orders WHERE id = ? FOR UPDATE",
                (rs, n) -> new OrderStatusAndTotal(OrderStatus.valueOf(rs.getString("status")), rs.getLong("total_cents")),
                id);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    public void updateStatus(UUID id, OrderStatus newStatus) {
        jdbc.update("UPDATE orders SET status = ?, version = version + 1, updated_at = ? WHERE id = ?",
                newStatus.name(), Timestamp.from(Instant.now()), id);
    }

    private static final RowMapper<Order> ORDER_ROW_MAPPER = (rs, rowNum) -> new Order(
            (UUID) rs.getObject("id"),
            (UUID) rs.getObject("customer_id"),
            OrderStatus.valueOf(rs.getString("status")),
            rs.getLong("total_cents"),
            rs.getString("currency"),
            rs.getInt("version"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant(),
            List.of());

    private static final RowMapper<OrderItem> ITEM_ROW_MAPPER = (rs, rowNum) -> new OrderItem(
            rs.getLong("id"),
            (UUID) rs.getObject("order_id"),
            rs.getString("sku"),
            rs.getInt("quantity"),
            rs.getLong("unit_price_cents"));
}
