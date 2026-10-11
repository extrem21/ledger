package dev.mahir.ledger.support;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Singleton-container pattern: one Postgres container for the whole JVM,
 * started once and never stopped (Ryuk reaps it at JVM exit). Deliberately
 * NOT using @Testcontainers/@Container — that pair scopes start/stop to each
 * test class's before/after-all, and since this field is inherited (shared)
 * across every IT subclass, a later subclass's afterAll would stop the
 * container out from under Spring contexts other subclasses already cached.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class IntegrationTestBase {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("ledger")
            .withUsername("ledger")
            .withPassword("ledger");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    protected JdbcTemplate jdbc;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("TRUNCATE TABLE processed_events, order_projection, outbox, idempotency_keys, order_items, orders RESTART IDENTITY CASCADE");
    }
}
