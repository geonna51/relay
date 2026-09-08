FROM rust:1.98-slim as builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/relay /usr/local/bin/relay
COPY --from=builder /app/target/release/relay-server /usr/local/bin/relay-server
COPY --from=builder /app/target/release/relay-worker /usr/local/bin/relay-worker

EXPOSE 8000
ENTRYPOINT ["relay"]
CMD ["server", "--host", "0.0.0.0", "--port", "8000"]
