ARG BASE_IMAGE
FROM ${BASE_IMAGE}
USER root
# Private qualification overlay: existing workers, current extension SQL/native.
COPY extensions/lake_replication /local-extension
COPY migrations /migrations
RUN make -C /local-extension clean with_llvm=no && make -C /local-extension -j2 install with_llvm=no
LABEL io.proddata.local-iceberg.test-only="current-core-existing-workers"
