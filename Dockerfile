# Multi-stage image for the vector tile pipeline binaries.
#
# The dependency tree is pure Rust — no protobuf compiler, no GDAL, no native
# libraries (see README "Generating MVT in Rust") — so the builder needs only
# the standard Rust image. `rust-toolchain.toml` pins the stable channel; the
# workspace MSRV is 1.75.
#
# Produces two binaries:
#   /usr/local/bin/vtile-api  — ingestion/job/catalog/tile HTTP API (TRD §8)
#   /usr/local/bin/vtile      — tile processor CLI (run/job-status/replay)
#
# The local mirror of the TRD §6 layout lives under /data; mount ./data there
# (docker compose does this) so host-side make targets see the same state.

FROM rust:slim AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p vtile-api -p vtile-pipeline

FROM debian:bookworm-slim
# ca-certificates only: needed if the image is ever pointed at real AWS/S3
# (the `aws` feature); harmless locally.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/vtile-api /usr/local/bin/vtile-api
COPY --from=builder /build/target/release/vtile /usr/local/bin/vtile

VOLUME ["/data"]
EXPOSE 8080
CMD ["vtile-api", "--data-dir", "/data", "--host", "0.0.0.0", "--port", "8080"]
