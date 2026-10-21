ARG QUAL_IMAGE
FROM ${QUAL_IMAGE}
ARG PROTOTYPE_HARDENED=0
ARG PROTOTYPE_VARIANT=base
USER root
COPY extensions/lake_replication /prototype/extensions/lake_replication
COPY migrations /prototype/migrations
COPY experiments/base_workers /prototype/experiment
RUN if [ "$PROTOTYPE_VARIANT" = static ]; then \
         python3 /prototype/experiment/adapt.py /prototype --hardened --static; \
    elif [ "$PROTOTYPE_HARDENED" = 1 ]; then \
         python3 /prototype/experiment/adapt.py /prototype --hardened; \
    else python3 /prototype/experiment/adapt.py /prototype; fi \
    && make -C /prototype/extensions/lake_replication clean all install \
    && if [ "$PROTOTYPE_VARIANT" = base ]; then \
       for version in 0.1.0-beta.1 0.1.0--0.1.0-beta.1; do \
         cat /prototype/experiment/register.sql \
           >> "$(pg_config --sharedir)/extension/lake_replication--$version.sql"; \
       done; fi
COPY experiments/base_workers/destination-init.sql /docker-entrypoint-initdb.d/20-qualification.sql
