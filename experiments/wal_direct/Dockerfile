ARG BASE_IMAGE=lake-local-iceberg:qualification-v8
FROM rust:1.85-bookworm@sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4 AS worker
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY worker ./worker
RUN cargo build --release --locked --features wal-direct-prototype --bins
FROM ${BASE_IMAGE}
# Include default workers so shared wire-client changes receive native regression coverage.
COPY --from=worker /src/target/release/lake-replication-direct /src/target/release/lake-replication-direct-init /src/target/release/lake-replication-receiver /src/target/release/lake-replication-publisher /src/target/release/lake-replication-initial-sync /src/target/release/lake-replication-supervisor /usr/local/bin/
LABEL io.proddata.wal-direct.experimental="true"
