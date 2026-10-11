CREATE TABLE orders (
    id              UUID PRIMARY KEY,
    customer_id     UUID        NOT NULL,
    status          TEXT        NOT NULL
                    CHECK (status IN ('PENDING','CONFIRMED','SHIPPED','CANCELLED')),
    total_cents     BIGINT      NOT NULL CHECK (total_cents >= 0),
    currency        CHAR(3)     NOT NULL DEFAULT 'INR',
    version         INT         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    id                BIGSERIAL PRIMARY KEY,
    order_id          UUID   NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    sku               TEXT   NOT NULL,
    quantity          INT    NOT NULL CHECK (quantity > 0),
    unit_price_cents  BIGINT NOT NULL CHECK (unit_price_cents >= 0)
);
CREATE INDEX idx_order_items_order ON order_items(order_id);

-- Idempotency: one row per client-supplied key.
-- request_fingerprint is a hash of the request body, used to detect
-- the same key being reused for a DIFFERENT request (a client bug).
CREATE TABLE idempotency_keys (
    key                  TEXT PRIMARY KEY,
    request_fingerprint  TEXT        NOT NULL,
    order_id             UUID        NOT NULL REFERENCES orders(id),
    response_status      INT         NOT NULL,
    response_body        JSONB       NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The transactional outbox.
CREATE TABLE outbox (
    id               BIGSERIAL PRIMARY KEY,
    aggregate_id     UUID        NOT NULL,
    event_type       TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    state            TEXT        NOT NULL DEFAULT 'PENDING'
                     CHECK (state IN ('PENDING','PUBLISHED','DEAD')),
    attempts         INT         NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at     TIMESTAMPTZ
);

-- Partial index: the drain query only ever looks at PENDING rows.
CREATE INDEX idx_outbox_claimable
    ON outbox (next_attempt_at, id)
    WHERE state = 'PENDING';

-- Consumer-side dedupe. Composite key: the same event may be
-- consumed by several independent consumers.
CREATE TABLE processed_events (
    event_id      BIGINT      NOT NULL,
    consumer      TEXT        NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer)
);

-- Read model built by the consumer — proves events are actually applied.
CREATE TABLE order_projection (
    order_id           UUID PRIMARY KEY,
    status             TEXT        NOT NULL,
    total_cents        BIGINT      NOT NULL,
    last_event_id      BIGINT      NOT NULL,
    last_updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
