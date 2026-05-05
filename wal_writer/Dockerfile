# DEPRECATED: Legacy Rust WAL Writer image
# The default WAL Writer runtime is now Go. This Dockerfile is kept only
# for backward compatibility and can be enabled via a deprecated profile.

# Stage 1: Build
FROM rust:1.88 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    clang \
    libclang-dev \
    pkg-config \
    libpq-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency files first for better layer caching
COPY wal_common/Cargo.toml wal_common/Cargo.toml
COPY wal_common/src wal_common/src
COPY wal_writer/Cargo.toml wal_writer/Cargo.toml
COPY wal_writer/Cargo.lock wal_writer/Cargo.lock

# Create a dummy src/main.rs to fetch dependencies
RUN mkdir -p wal_writer/src && \
    echo "fn main() {}" > wal_writer/src/main.rs && \
    echo "" > wal_writer/src/lib.rs

# Fetch dependencies
RUN cargo fetch --manifest-path wal_writer/Cargo.toml

# Copy source code
COPY wal_writer/src wal_writer/src

# Build release binary
RUN cargo build --release --manifest-path wal_writer/Cargo.toml && \
    strip /app/wal_writer/target/release/rust-wal-cake-writer

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/wal_writer/target/release/rust-wal-cake-writer /app/rust-wal-cake-writer

# Create log directory
RUN mkdir -p /var/log/wal-writer

# Expose metrics port
EXPOSE 9090

# Health check (matches K8s livenessProbe and readinessProbe)
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:9090/health || exit 1

# Run the binary
ENTRYPOINT ["/app/rust-wal-cake-writer"]
