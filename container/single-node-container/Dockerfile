FROM ubuntu:24.04@sha256:496754492fb28b4d3049432f2ca787449331e23fb14f0dd3fffea86bf5a93eb4 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends binutils ca-certificates curl git patchelf && rm -rf /var/lib/apt/lists/*
ARG PIXI_VERSION=v0.77.0
ARG PIXI_SHA256=6e2183fafd5f1750675c0adf4d6b3d6a1d997346043f90c0cb41bc7eb5c34078
RUN curl -fsSL --retry 3 -o /usr/local/bin/pixi "https://github.com/prefix-dev/pixi/releases/download/${PIXI_VERSION}/pixi-x86_64-unknown-linux-musl" \
    && echo "${PIXI_SHA256}  /usr/local/bin/pixi" | sha256sum -c - \
    && chmod 0755 /usr/local/bin/pixi

WORKDIR /src
COPY pixi.toml pixi.lock ./
RUN pixi install --locked -e default
COPY . .
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN pixi run cargo build --release -p crowdb-kv-client --features ffi
RUN pixi run build-cpp
RUN pixi run cargo build --release \
    -p crowdb-monitor -p crowdb-kv-server -p crowdb-diskdb \
    -p crowdb-chunkdb -p crowdb-chunk-kv-server \
    -p crowdb-access-server -p crowdb-web
RUN pixi run bash -c 'cd app/crowdb-web/ui && npm ci && npm run build'
RUN pixi run bash container/single-node-container/collect-libs.sh

FROM ubuntu:24.04@sha256:496754492fb28b4d3049432f2ca787449331e23fb14f0dd3fffea86bf5a93eb4
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libcap2-bin && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 crowdb \
    && useradd --system --uid 10001 --gid 10001 --home-dir /opt/crowdb --shell /usr/sbin/nologin crowdb
RUN --mount=type=bind,from=builder,source=/src/preview-runtime,target=/staged,ro \
    mkdir -p /opt/crowdb \
    && cp -a /staged/bin /staged/lib /opt/crowdb/ \
    && setcap cap_net_bind_service=+ep /opt/crowdb/bin/crowdb-iceberg \
    && mkdir -p /opt/crowdb/data /opt/crowdb/run \
    && chown -R crowdb:crowdb /opt/crowdb/data /opt/crowdb/run
COPY --from=builder /src/app/crowdb-web/ui/dist/ /opt/crowdb/ui/
COPY --from=builder /src/container/single-node-container/profile.toml /opt/crowdb/etc/profile.toml
COPY --from=builder /src/container/single-node-container/templates/ /opt/crowdb/etc/templates/
COPY container/single-node-container/entrypoint.sh /opt/crowdb/bin/entrypoint
RUN chmod 0755 /opt/crowdb/bin/entrypoint

ARG SOURCE_REVISION
ARG PREVIEW_VERSION
RUN test -n "$SOURCE_REVISION" && test -n "$PREVIEW_VERSION"
LABEL org.opencontainers.image.title="CROWDB Single-Node Container" \
      org.opencontainers.image.description="Non-production single-node storage preview" \
      org.opencontainers.image.revision="$SOURCE_REVISION" \
      org.opencontainers.image.version="$PREVIEW_VERSION"
ENV PATH="/opt/crowdb/bin:${PATH}" \
    LD_LIBRARY_PATH="/opt/crowdb/lib" \
    CROWDB_RUNTIME_ROOT="/opt/crowdb/run"
USER crowdb:crowdb
VOLUME ["/opt/crowdb/data"]
EXPOSE 80 8010 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=5s --start-period=120s --retries=3 CMD crowdb-monitor liveness && crowdb-monitor readiness
ENTRYPOINT ["/opt/crowdb/bin/entrypoint"]
