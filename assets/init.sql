CREATE UNLOGGED TABLE PAYMENTS (
    correlation_id CHAR(36),
    amount DECIMAL(10, 2),
    payment_processor CHAR(1),
    requested_at TIMESTAMP
);