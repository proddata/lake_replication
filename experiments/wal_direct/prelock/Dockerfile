ARG BASE_IMAGE=lake-local-iceberg:wal-direct-v13
FROM ${BASE_IMAGE}
USER root
COPY experiments/wal_direct/prelock/patch.py /opt/lake-direct-prelock-patch.py
RUN python3 /opt/lake-direct-prelock-patch.py
LABEL io.proddata.wal-direct.prelock-prototype="native-table-lock-before-publication-writes"
