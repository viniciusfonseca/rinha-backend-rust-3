CREATE UNLOGGED TABLE PAYMENTS (
    correlation_id CHAR(36),
    amount DOUBLE PRECISION,
    payment_processor CHAR(1),
    requested_at TIMESTAMP
);

CREATE INDEX requested_at_idx ON PAYMENTS (requested_at);
CREATE INDEX payment_processor_idx ON PAYMENTS (payment_processor);