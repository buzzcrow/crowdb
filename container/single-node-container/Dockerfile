FROM ubuntu:24.04@sha256:496754492fb28b4d3049432f2ca787449331e23fb14f0dd3fffea86bf5a93eb4
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 crowdb \
    && useradd --system --uid 10001 --gid 10001 --home-dir /opt/crowdb --shell /usr/sbin/nologin crowdb
ARG SOURCE_REVISION
ARG PREVIEW_VERSION
ARG RUNTIME_SHA256
RUN --mount=type=bind,source=.,target=/staged,ro \
    test -n "$SOURCE_REVISION" && test -n "$PREVIEW_VERSION" \
    && test "$(cat /staged/SOURCE_REVISION)" = "$SOURCE_REVISION" \
    && test "$(cat /staged/VERSION)" = "$PREVIEW_VERSION" \
    && mkdir -p /opt/crowdb/etc /opt/crowdb/data /opt/crowdb/run \
    && cp -a /staged/bin /staged/lib /staged/ui /opt/crowdb/ \
    && cp -a /staged/templates /opt/crowdb/etc/ \
    && cp /staged/profile.toml /opt/crowdb/etc/profile.toml \
    && cp /staged/entrypoint.sh /opt/crowdb/bin/entrypoint \
    && chown -R crowdb:crowdb /opt/crowdb/data /opt/crowdb/run \
    && chmod 0755 /opt/crowdb/bin/entrypoint \
    && for binary in /opt/crowdb/bin/crowdb-*; do \
        LD_LIBRARY_PATH=/opt/crowdb/lib ldd "$binary" > /tmp/dependencies.txt 2>&1 || { cat /tmp/dependencies.txt; exit 1; }; \
        if grep -q 'not found' /tmp/dependencies.txt; then cat /tmp/dependencies.txt; exit 1; fi; \
    done && rm /tmp/dependencies.txt

LABEL org.opencontainers.image.title="CROWDB Iceberg" \
      org.opencontainers.image.description="Apache Iceberg REST catalog with native storage, in one container for development and testing." \
      org.opencontainers.image.url="https://crowdb.dev/" \
      org.opencontainers.image.source="https://github.com/buzzcrow/crowdb" \
      org.opencontainers.image.authors="Gian <crow.db@outlook.com>" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="$SOURCE_REVISION" \
      org.opencontainers.image.version="$PREVIEW_VERSION" \
      org.crowdb.runtime.sha256="$RUNTIME_SHA256"
ENV PATH="/opt/crowdb/bin:${PATH}" \
    LD_LIBRARY_PATH="/opt/crowdb/lib" \
    CROWDB_RUNTIME_ROOT="/opt/crowdb/run"
USER crowdb:crowdb
VOLUME ["/opt/crowdb/data"]
EXPOSE 9090 9091 9092
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=5s --start-period=120s --retries=3 CMD crowdb-monitor liveness && crowdb-monitor readiness
ENTRYPOINT ["/opt/crowdb/bin/entrypoint"]
