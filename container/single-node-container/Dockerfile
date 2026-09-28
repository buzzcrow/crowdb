FROM ubuntu:24.04@sha256:496754492fb28b4d3049432f2ca787449331e23fb14f0dd3fffea86bf5a93eb4
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libcap2-bin && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 crowdb \
    && useradd --system --uid 10001 --gid 10001 --home-dir /opt/crowdb --shell /usr/sbin/nologin crowdb
RUN --mount=type=bind,source=.,target=/staged,ro \
    mkdir -p /opt/crowdb \
    && cp -a /staged/bin /staged/lib /opt/crowdb/ \
    && setcap cap_net_bind_service=+ep /opt/crowdb/bin/crowdb-iceberg \
    && setcap cap_net_bind_service=+ep /opt/crowdb/bin/crowdb-access-server \
    && mkdir -p /opt/crowdb/data /opt/crowdb/run \
    && chown -R crowdb:crowdb /opt/crowdb/data /opt/crowdb/run
COPY ui/ /opt/crowdb/ui/
COPY profile.toml /opt/crowdb/etc/profile.toml
COPY templates/ /opt/crowdb/etc/templates/
COPY entrypoint.sh /opt/crowdb/bin/entrypoint
RUN chmod 0755 /opt/crowdb/bin/entrypoint \
    && for binary in /opt/crowdb/bin/crowdb-*; do \
        LD_LIBRARY_PATH=/opt/crowdb/lib ldd "$binary" > /tmp/dependencies.txt 2>&1 || { cat /tmp/dependencies.txt; exit 1; }; \
        if grep -q 'not found' /tmp/dependencies.txt; then cat /tmp/dependencies.txt; exit 1; fi; \
    done && rm /tmp/dependencies.txt

ARG SOURCE_REVISION
ARG PREVIEW_VERSION
RUN --mount=type=bind,source=.,target=/staged,ro \
    test -n "$SOURCE_REVISION" && test -n "$PREVIEW_VERSION" \
    && test "$(cat /staged/SOURCE_REVISION)" = "$SOURCE_REVISION" \
    && test "$(cat /staged/VERSION)" = "$PREVIEW_VERSION"
LABEL org.opencontainers.image.title="CROWDB Iceberg" \
      org.opencontainers.image.description="Apache Iceberg REST catalog with native storage, in one container for development and testing." \
      org.opencontainers.image.url="https://crowdb.dev/" \
      org.opencontainers.image.source="https://github.com/buzzcrow/crowdb" \
      org.opencontainers.image.documentation="https://github.com/buzzcrow/crowdb/blob/v${PREVIEW_VERSION}/doc/user-manual/docker-single-node-user-guide.md" \
      org.opencontainers.image.authors="Gian <crow.db@outlook.com>" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="$SOURCE_REVISION" \
      org.opencontainers.image.version="$PREVIEW_VERSION"
ENV PATH="/opt/crowdb/bin:${PATH}" \
    LD_LIBRARY_PATH="/opt/crowdb/lib" \
    CROWDB_RUNTIME_ROOT="/opt/crowdb/run"
USER crowdb:crowdb
VOLUME ["/opt/crowdb/data"]
EXPOSE 80 81 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=5s --start-period=120s --retries=3 CMD crowdb-monitor liveness && crowdb-monitor readiness
ENTRYPOINT ["/opt/crowdb/bin/entrypoint"]
