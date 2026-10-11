package dev.mahir.ledger.domain;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class StateMachineTest {

    @ParameterizedTest
    @CsvSource({
            "PENDING, CONFIRMED, true",
            "PENDING, CANCELLED, true",
            "PENDING, SHIPPED, false",
            "PENDING, PENDING, false",
            "CONFIRMED, SHIPPED, true",
            "CONFIRMED, CANCELLED, true",
            "CONFIRMED, PENDING, false",
            "SHIPPED, PENDING, false",
            "SHIPPED, CONFIRMED, false",
            "SHIPPED, CANCELLED, false",
            "CANCELLED, PENDING, false",
            "CANCELLED, CONFIRMED, false",
            "CANCELLED, SHIPPED, false",
    })
    void transitions(OrderStatus from, OrderStatus to, boolean expectedLegal) {
        assertThat(StateMachine.isLegal(from, to)).isEqualTo(expectedLegal);
    }
}
