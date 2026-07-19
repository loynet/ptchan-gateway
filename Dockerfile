ARG RUST_VERSION=1.93.1
FROM rust:${RUST_VERSION}-bookworm AS build

WORKDIR /src

COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates \
	&& rm -rf /var/lib/apt/lists/* \
	&& mkdir -p /data /etc/ptchan-gateway \
	&& chown -R 65532:65532 /data /etc/ptchan-gateway
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /src/target/release/ptchan-gateway /usr/local/bin/ptchan-gateway

USER 65532:65532

ENV SQLITE_PATH=/data/ptchan-gateway.db
ENV CONFIG_FILE=/etc/ptchan-gateway/config.toml

STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
	CMD ["/usr/local/bin/ptchan-gateway", "--check-health"]

ENTRYPOINT ["/usr/local/bin/ptchan-gateway"]
