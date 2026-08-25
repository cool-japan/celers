# Multi-stage Dockerfile for CeleRS
#
# Note: this image packages the `celers` CLI binary only -- inspect, control,
# queue, dlq, schedule, backup/restore, doctor, and friends. CeleRS tasks are
# compiled-in Rust impls (unlike Python Celery, which imports task modules at
# runtime), so `celers worker start` from this image runs with an EMPTY task
# registry and cannot execute any user task. To run tasks, link
# `celers-worker` into your own binary, register your tasks, and build your
# own image FROM this builder stage (or an equivalent one). See
# docs/DEPLOYMENT.md's "Building a worker image" section for a worked
# example.

# Build stage
FROM rust:1.95.0-slim-bookworm AS builder

WORKDIR /app

# CeleRS is Pure Rust with its default feature set (rustls + the
# rustls-rustcrypto provider via oxitls; see the workspace Cargo.toml's
# Pure-Rust policy notes and deny.toml's ban on openssl / openssl-sys), so no
# pkg-config/libssl-dev is needed to build the `celers` binary below.
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/

RUN cargo build --release -p celers-cli

# Runtime stage
FROM debian:bookworm-slim

ARG CELERS_VERSION=0.3.1

# ca-certificates only: rustls with webpki-roots does not read the system
# trust store, but keeping the system CA bundle costs nothing and helps any
# future tooling added to the image. No OpenSSL packages are installed here
# -- see deny.toml's ban on openssl / openssl-sys.
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 celers && \
    mkdir -p /data && \
    chown -R celers:celers /data

# Copy binary from builder. crates/celers-cli/Cargo.toml declares
# `[[bin]] name = "celers"`, so the produced artifact is target/release/celers
# (there is no target/release/celers-cli).
COPY --from=builder /app/target/release/celers /usr/local/bin/celers

# Switch to non-root user
USER celers

# Set working directory
WORKDIR /data

# Environment variables
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

# Default command
ENTRYPOINT ["celers"]
CMD ["--help"]

# Metadata
LABEL org.opencontainers.image.source="https://github.com/cool-japan/celers"
LABEL org.opencontainers.image.description="CeleRS - Distributed Task Queue for Rust"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.version="${CELERS_VERSION}"
