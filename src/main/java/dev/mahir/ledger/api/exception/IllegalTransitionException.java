package dev.mahir.ledger.api.exception;

import dev.mahir.ledger.domain.OrderStatus;

public class IllegalTransitionException extends RuntimeException {
    public IllegalTransitionException(OrderStatus from, OrderStatus to) {
        super("Cannot transition order from " + from + " to " + to);
    }
}
