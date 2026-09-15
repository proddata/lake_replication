FROM postgres:18-bookworm@sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af

ARG PATRONI_VERSION=4.1.5

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        python3 \
        python3-pip \
        python3-psycopg2 \
        curl \
    && python3 -m pip install --break-system-packages --no-cache-dir \
        "patroni[etcd3]==${PATRONI_VERSION}" \
    && rm -rf /var/lib/apt/lists/*

USER postgres
ENTRYPOINT ["patroni"]
