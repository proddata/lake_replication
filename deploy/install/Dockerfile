# Produces a filesystem artifact only. It does not bundle PostgreSQL or pg_lake.
FROM rust:1.85-bookworm@sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4 AS worker-builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY worker ./worker
RUN cargo build --release --locked --bins \
    && strip target/release/lake-replication-receiver \
             target/release/lake-replication-publisher \
             target/release/lake-replication-initial-sync \
             target/release/lake-replication-supervisor

FROM postgres:18-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af AS extension-builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential postgresql-server-dev-18 \
    && rm -rf /var/lib/apt/lists/*
COPY extensions/lake_replication /src/extensions/lake_replication
COPY migrations /src/migrations
RUN make -C /src/extensions/lake_replication all \
    && strip --strip-unneeded /src/extensions/lake_replication/lake_replication.so

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251 AS assembler
ARG TARGETARCH
ARG VERSION=0.1.0-beta.1
ARG REVISION=unknown
WORKDIR /bundle/pg_lake_replication-${VERSION}
COPY --from=worker-builder \
    /src/target/release/lake-replication-receiver \
    /src/target/release/lake-replication-publisher \
    /src/target/release/lake-replication-initial-sync \
    /src/target/release/lake-replication-supervisor \
    payload/bin/
COPY --from=extension-builder \
    /src/extensions/lake_replication/lake_replication.so \
    payload/lib/
COPY --from=extension-builder \
    /src/extensions/lake_replication/lake_replication.control \
    /src/extensions/lake_replication/lake_replication--0.1.0.sql \
    /src/extensions/lake_replication/lake_replication--0.1.0-beta.1.sql \
    /src/extensions/lake_replication/lake_replication--0.1.0--0.1.0-beta.1.sql \
    payload/extension/
COPY deploy/install/install.sh deploy/install/uninstall.sh \
     deploy/install/README.md deploy/install/COMPATIBILITY.md ./
COPY docs/upgrade-and-rollback.md UPGRADE.md
COPY LICENSE ./
RUN printf '%s\n' "$VERSION" > VERSION \
    && printf '%s\n' "$REVISION" > SOURCE_REVISION \
    && printf '%s\n' "$TARGETARCH" > TARGETARCH \
    && find . -type f ! -name SHA256SUMS -print0 \
       | sort -z \
       | xargs -0 sha256sum > SHA256SUMS
RUN cd /bundle \
    && tar --sort=name --mtime='@0' --owner=0 --group=0 --numeric-owner \
       -cf - pg_lake_replication-${VERSION} \
       | gzip -n > pg_lake_replication-${VERSION}.tar.gz \
    && sha256sum pg_lake_replication-${VERSION}.tar.gz \
       > pg_lake_replication-${VERSION}.tar.gz.sha256

FROM scratch AS bundle
ARG VERSION=0.1.0-beta.1
COPY --from=assembler /bundle/pg_lake_replication-${VERSION} /pg_lake_replication-${VERSION}
COPY --from=assembler /bundle/pg_lake_replication-${VERSION}.tar.gz /pg_lake_replication-${VERSION}.tar.gz
COPY --from=assembler /bundle/pg_lake_replication-${VERSION}.tar.gz.sha256 /pg_lake_replication-${VERSION}.tar.gz.sha256
