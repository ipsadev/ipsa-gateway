FROM rust:1.88-slim-bookworm AS builder

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        make \
        gcc \
        libc-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY . .

RUN cargo build --release --bin ipsa-gateway

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        curl \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

ADD --chmod=644 https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /etc/ssl/rds-global-bundle.pem
ENV PGSSLROOTCERT=/etc/ssl/rds-global-bundle.pem

WORKDIR /app
COPY --from=builder /build/target/release/ipsa-gateway /usr/local/bin/ipsa-gateway

EXPOSE 50052

ENV STELLAR_GATEWAY_HOST=0.0.0.0

ENTRYPOINT ["ipsa-gateway"]
