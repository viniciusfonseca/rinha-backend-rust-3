CREATE TABLE payments_default (
    amount DECIMAL NOT NULL,
    requested_at TIMESTAMPTZ PRIMARY KEY
)
WITH (
    timescaledb.hypertable,
    timescaledb.partition_column = 'requested_at'
);

CREATE TABLE payments_fallback (
    amount DECIMAL NOT NULL,
    requested_at TIMESTAMPTZ PRIMARY KEY
)
WITH (
    timescaledb.hypertable,
    timescaledb.partition_column = 'requested_at'
);

-- SELECT set_chunk_time_interval('payments', 1000);