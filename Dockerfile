# Multi-stage Dockerfile for CeleRS
#
# Note: this image packages the `celers` CLI binary only -- inspect, control,
# queue, dlq, schedule, backup/restore, doctor, and friends. CeleRS tasks are
# compiled-in Rust impls (unlike Python Celery, which imports task modules at
# runtime), so `celers worker` from this image runs with an EMPTY task
# registry and cannot execute any user task. To run tasks, link
# `celers-worker` into your own binary, register your tasks, and build your
# own image FROM this builder stage (or an equivalent one). See
# docs/DEPLOYMENT.md's "Building a Worker Image" section for a worked
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

# Deliberately NOT installing ca-certificates here. `celers-cli`'s default
# build resolves TLS entirely through oxitls' `webpki-roots` feature
# (Mozilla's compiled-in root bundle via the `webpki-roots` crate --
# confirm with `cargo tree -p celers-cli | grep webpki-roots`), so it never
# reads /etc/ssl/certs and the system CA bundle buys nothing. It is worse
# than useless: on Debian bookworm, `ca-certificates` carries a hard
# `Depends: openssl` (verified with `apt-cache depends ca-certificates`,
# and empirically -- `apt-get install ca-certificates` here pulls in both
# `openssl` and `libssl3` even with --no-install-recommends). Installing it
# would silently defeat the Pure-Rust story deny.toml's ban on openssl /
# openssl-sys enforces at the Cargo level, by putting OpenSSL in the image
# through the OS package manager instead. If a future feature needs the
# system trust store (celers-broker-amqp's `tls-native-certs`, or a
# downstream worker image with different TLS needs), install
# `ca-certificates` there deliberately and accept the OpenSSL package that
# comes with it -- do not add it here "just in case".

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
