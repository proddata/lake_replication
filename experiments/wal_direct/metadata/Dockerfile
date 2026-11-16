ARG BASE_IMAGE=lake-local-iceberg:wal-direct-v11
FROM ${BASE_IMAGE}
USER root
COPY experiments/wal_direct/metadata/patch.py /opt/lake-direct-metadata-patch.py
RUN python3 /opt/lake-direct-metadata-patch.py
LABEL io.proddata.wal-direct.metadata-prototype="skip-unused-position-delete-column-statistics"
