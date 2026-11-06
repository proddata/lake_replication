ARG BASE_IMAGE=pg-lake-qualification-base:8180647d4535
FROM ${BASE_IMAGE}
USER root
COPY . /opt/lake-replication-pg-lake
RUN python3 /opt/lake-replication-pg-lake/patchset.py install --source /opt/pg_lake
LABEL io.proddata.pg-lake.patchset="lake-replication-manifest-cleanup-v1"
