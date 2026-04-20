# Multi-stage Dockerfile for CeleRS
# Build stage
FROM rust:1.95-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/

# Build dependencies separately for caching
RUN mkdir -p examples benches && \
    cargo build --release --workspace && \
    rm -rf target/release/deps/celers*

# Build the actual application
COPY . .
RUN cargo build --release -p celers-cli

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 celers && \
    mkdir -p /data && \
    chown -R celers:celers /data

# Copy binary from builder
COPY --from=builder /app/target/release/celers-cli /usr/local/bin/celers

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
LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"
