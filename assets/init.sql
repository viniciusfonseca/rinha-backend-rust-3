CREATE UNLOGGED TABLE payments (
    id SERIAL PRIMARY KEY,
    amount DECIMAL NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    payment_processor_id CHAR(1) NOT NULL
);

CREATE INDEX payments_processor_id ON payments USING HASH(payment_processor_id);
CREATE INDEX payments_requested_at ON payments(requested_at);