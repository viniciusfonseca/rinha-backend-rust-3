FROM rust:1.88 AS builder
WORKDIR /app
COPY . .
RUN rustup target add x86_64-unknown-linux-musl
RUN RUSTFLAGS="--cfg tokio_unstable" cargo build --release --target x86_64-unknown-linux-musl

FROM scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rinha-backend-rust-3 .
CMD ["./rinha-backend-rust-3"]