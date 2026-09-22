# PoC/demo image: same pg_lake build as the milestone lab, without the
# lab-only crash-injection patch, plus this project's own worker binaries
# and the lake_subscription_sugar CREATE-SUBSCRIPTION wrapper baked in.
# Not hardened for production; see deploy/poc/README.md.

FROM rust:bookworm AS worker-builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY worker ./worker
RUN cargo build --release --bins

FROM postgres:18-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af AS pg-lake-base
ARG PG_LAKE_REV=8180647d4535fdb1ed223f92f301ba6094a8ec3e
ARG BUILD_JOBS=2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake ninja-build git curl ca-certificates pkg-config \
    python3-dev bison flex libreadline-dev zlib1g-dev libxml2-dev libxslt1-dev \
    libicu-dev libssl-dev liblz4-dev liblzma-dev libsnappy-dev libtool \
    libjansson-dev libcurl4-openssl-dev libzstd-dev zip unzip tar
WORKDIR /opt/pg_lake
RUN git init && git remote add origin https://github.com/Snowflake-Labs/pg_lake.git \
    && git fetch --depth 1 origin ${PG_LAKE_REV} && git checkout --detach FETCH_HEAD \
    && test "$(git rev-parse HEAD)" = "${PG_LAKE_REV}"
RUN git clone --depth 1 --branch 2025.10.17 https://github.com/microsoft/vcpkg.git /opt/vcpkg \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics
ENV VCPKG_TOOLCHAIN_PATH=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake
ENV VCPKG_FORCE_SYSTEM_BINARIES=1
ENV PGCOMPAT_BUILD_CONFIG=Release
RUN VCPKG_MAX_CONCURRENCY=${BUILD_JOBS} /opt/vcpkg/vcpkg install \
    azure-identity-cpp azure-storage-blobs-cpp azure-storage-files-datalake-cpp openssl
RUN apt-get install -y --no-install-recommends postgresql-server-dev-18 libkrb5-dev
RUN git submodule update --init --depth 1 \
    && git -C duckdb_pglake/duckdb-postgres submodule update --init --depth 1 database-connector
RUN make -j${BUILD_JOBS} install-pg_lake with_llvm=no
# Preserve release identification for runtime extension downloads in shallow clones.
RUN git -C duckdb_pglake/duckdb fetch --depth 1 origin tag v1.5.5 \
    && test "$(git -C duckdb_pglake/duckdb rev-parse v1.5.5)" = "$(git -C duckdb_pglake/duckdb rev-parse HEAD)" \
    && make -C duckdb_pglake patch_duckdb
# Cache dependency configuration independently of the expensive C++ compilation.
RUN --mount=type=cache,id=lake-poc-vcpkg,target=/root/.cache/vcpkg \
    VCPKG_MAX_CONCURRENCY=${BUILD_JOBS} cmake \
    -S duckdb_pglake/duckdb -B duckdb_pglake/build/release \
    -DCMAKE_BUILD_TYPE=Release -DEXTENSION_STATIC_BUILD=1 \
    -DDUCKDB_EXTENSION_CONFIGS=/opt/pg_lake/duckdb_pglake/extension_config.cmake \
    -DPG_INCLUDEDIR="$(pg_config --includedir)" -DPG_LIBDIR="$(pg_config --libdir)" \
    '-DBUILD_EXTENSIONS=tpch;json;icu' -DBUILD_UNITTESTS=OFF -DBUILD_SHELL=OFF \
    -DVCPKG_MANIFEST_DIR=/opt/pg_lake/duckdb_pglake -DVCPKG_BUILD=1 \
    -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake
RUN cmake --build duckdb_pglake/build/release -j${BUILD_JOBS} \
    && cp duckdb_pglake/build/release/src/libduckdb.so duckdb_pglake/libduckdb.so \
    && make -C duckdb_pglake install
RUN apt-get install -y --no-install-recommends libselinux1-dev libpam0g-dev libnuma-dev \
    && make install-pgduck_server DUCKDB_BUILD_USE_CACHE=1 NCORES=${BUILD_JOBS} \
       with_llvm=no LDFLAGS="-L$(pg_config --pkglibdir)"
RUN ldconfig
LABEL org.opencontainers.image.source="https://github.com/Snowflake-Labs/pg_lake" \
      org.opencontainers.image.revision=${PG_LAKE_REV}

# This project's own control-plane wrapper: CREATE/ALTER/DROP SUBSCRIPTION
# ... WITH (lake_target = 'iceberg') spawns the worker binaries below and
# routes to lake_replication.* instead of native logical replication.
COPY extensions/lake_subscription_sugar /tmp/lake_subscription_sugar
RUN make -C /tmp/lake_subscription_sugar all install \
    && rm -rf /tmp/lake_subscription_sugar

COPY --from=worker-builder \
    /src/target/release/lake-replication-receiver \
    /src/target/release/lake-replication-publisher \
    /src/target/release/lake-replication-initial-sync \
    /src/target/release/lake-replication-supervisor \
    /usr/local/bin/

WORKDIR /poc
