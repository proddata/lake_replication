ARG BASE_IMAGE=lake-local-iceberg:wal-direct-v11
FROM ${BASE_IMAGE}
USER root
COPY experiments/wal_direct/catalog_profile/patch.py /opt/lake-direct-catalog-profile-patch.py
RUN python3 /opt/lake-direct-catalog-profile-patch.py
LABEL io.proddata.wal-direct.catalog-profile="diagnostic-only"
