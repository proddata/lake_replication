ARG BASE_IMAGE
FROM rust:1.85-bookworm@sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4 AS workers
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY worker ./worker
RUN cargo build --release --locked --workspace

FROM ${BASE_IMAGE}
USER root
# Private qualification overlay; no host installation or release artifact.
COPY --from=workers /src/target/release/lake-replication-receiver /src/target/release/lake-replication-supervisor /src/target/release/lake-replication-publisher /src/target/release/lake-replication-initial-sync /usr/local/bin/
COPY extensions/lake_replication /local-extension
COPY migrations /migrations
RUN make -C /local-extension -j2 install with_llvm=no
LABEL io.proddata.local-iceberg.test-only="current-workers-and-core"
