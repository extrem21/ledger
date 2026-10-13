package dev.mahir.ledger.domain;

import java.util.Map;
import java.util.Set;

/**
 * The order lifecycle, encoded as one explicit transition table rather than
 * scattered conditionals. See the README diagram for the full flow.
 */
public final class StateMachine {

    private static final Map<OrderStatus, Set<OrderStatus>> LEGAL_TRANSITIONS = Map.of(
            OrderStatus.PENDING, Set.of(OrderStatus.CONFIRMED, OrderStatus.CANCELLED),
            OrderStatus.CONFIRMED, Set.of(OrderStatus.SHIPPED, OrderStatus.CANCELLED),
            OrderStatus.SHIPPED, Set.of(),
            OrderStatus.CANCELLED, Set.of()
    );

    private StateMachine() {
    }

    public static boolean isLegal(OrderStatus from, OrderStatus to) {
        return LEGAL_TRANSITIONS.getOrDefault(from, Set.of()).contains(to);
    }
}
