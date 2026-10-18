# Development image; retains toolchains and upstream source for investigation.
FROM postgres:18-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af
ARG PG_LAKE_REV=8180647d4535fdb1ed223f92f301ba6094a8ec3e
ARG BUILD_JOBS=2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake ninja-build git curl ca-certificates pkg-config \
    python3-dev bison flex libreadline-dev zlib1g-dev libxml2-dev libxslt1-dev \
    libicu-dev libssl-dev liblz4-dev liblzma-dev libsnappy-dev libtool \
    libjansson-dev libcurl4-openssl-dev libzstd-dev zip unzip tar
COPY .upstream/pg_lake.bundle /tmp/pg_lake.bundle
RUN git clone /tmp/pg_lake.bundle /opt/pg_lake \
    && git -C /opt/pg_lake checkout --detach ${PG_LAKE_REV} \
    && git -C /opt/pg_lake remote set-url origin https://github.com/Snowflake-Labs/pg_lake.git \
    && test "$(git -C /opt/pg_lake rev-parse HEAD)" = "${PG_LAKE_REV}" \
    && rm /tmp/pg_lake.bundle
WORKDIR /opt/pg_lake
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
RUN --mount=type=cache,id=lake-lab-vcpkg,target=/root/.cache/vcpkg \
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
RUN apt-get update \
    && apt-get install -y --no-install-recommends libselinux1-dev libpam0g-dev libnuma-dev \
    && make install-pgduck_server DUCKDB_BUILD_USE_CACHE=1 NCORES=${BUILD_JOBS} \
       with_llvm=no LDFLAGS="-L$(pg_config --pkglibdir)"
COPY deploy/docker-compose/patches/pg-lake-lab-crash-points.patch /tmp/pg-lake-lab-crash-points.patch
RUN git apply --check /tmp/pg-lake-lab-crash-points.patch \
    && git apply /tmp/pg-lake-lab-crash-points.patch \
    && make -C pg_lake_table -j${BUILD_JOBS} install with_llvm=no
RUN ldconfig

# lake_replication must exist on disk before the first postmaster
# startup: shared_preload_libraries (see compose.yml) refuses to start if a
# listed library is missing, and adding it there only takes effect on a
# restart, not a bare CREATE EXTENSION -- see Milestone 27's addendum.
# tests/postgres_integration/probe.py rebuilds and reinstalls this from the
# current working tree on every run (for fast iteration without a full
# image rebuild) and restarts postgres itself to pick that up; this baked-in
# copy only needs to be correct as of the last `make lab-build`.
COPY migrations /migrations
COPY extensions/lake_replication /tmp/lake_replication
RUN make -C /tmp/lake_replication all install \
    && rm -rf /tmp/lake_replication

LABEL org.opencontainers.image.source="https://github.com/Snowflake-Labs/pg_lake" \
      org.opencontainers.image.revision=${PG_LAKE_REV}
WORKDIR /lab
