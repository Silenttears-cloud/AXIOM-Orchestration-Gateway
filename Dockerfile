# ==============================================================================
# STAGE 1: Build the React Dashboard Frontend
# ==============================================================================
FROM node:20-alpine AS frontend-builder
WORKDIR /app
COPY dashboard-ui/package*.json ./dashboard-ui/
WORKDIR /app/dashboard-ui
RUN npm ci
COPY dashboard-ui/ ./
RUN npm run build

# ==============================================================================
# STAGE 2: Build the Rust Backend Binary
# ==============================================================================
FROM rust:latest AS backend-builder

WORKDIR /usr/src/axiom-gateway
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
# Copy the compiled dashboard assets from Stage 1 to the exact folder expected by rust-embed
COPY --from=frontend-builder /app/dashboard-ui/dist ./dashboard-ui/dist
# Compile the Rust application in release mode
RUN cargo build --release

# ==============================================================================
# STAGE 3: Secure, Lightweight Production Runtime
# ==============================================================================
FROM debian:bookworm-slim
WORKDIR /app

# Install runtime dependencies (OpenSSL, CA-certificates for TLS upstream, and SQLite)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libsqlite3-0 \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Copy the compiled release binary from Stage 2
COPY --from=backend-builder /usr/src/axiom-gateway/target/release/axiom-gateway ./axiom-gateway

# Copy default config file
COPY config.yaml ./config.yaml

# Expose proxy and dashboard port
EXPOSE 8080

# Run the binary
CMD ["./axiom-gateway"]
