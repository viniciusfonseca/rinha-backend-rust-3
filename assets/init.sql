CREATE UNLOGGED TABLE payments (
	id SERIAL PRIMARY KEY,
    amount DECIMAL NOT NULL,
    requested_at TIMESTAMP NOT NULL,
    processor CHAR(1) NOT NULL
);

CREATE INDEX payments_processor ON payments(processor);
CREATE INDEX payments_requested_at ON payments(requested_at);