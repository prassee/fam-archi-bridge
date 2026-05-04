FROM rust:1.88 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    libsasl2-dev \
    zlib1g-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY wal_common/Cargo.toml wal_common/Cargo.toml
COPY wal_common/src wal_common/src
COPY wal_consumer/Cargo.toml wal_consumer/Cargo.toml
COPY wal_consumer/src wal_consumer/src

RUN cargo build --release --manifest-path wal_consumer/Cargo.toml

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    libsasl2-2 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/wal_consumer/target/release/wal_consumer /app/wal_consumer

ENTRYPOINT ["/app/wal_consumer"]