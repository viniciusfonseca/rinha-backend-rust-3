CREATE TABLE payments (
    amount DECIMAL NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    payment_processor_id CHAR(1) NOT NULL
)
WITH (
    timescaledb.hypertable,
    timescaledb.partition_column = 'requested_at',
    timescaledb.segmentby = 'payment_processor_id'
);

-- SELECT set_chunk_time_interval('payments', 1000);