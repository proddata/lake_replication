#!/bin/bash
set -euo pipefail

if [[ ! -s /certs/server.key ]]; then
  openssl req -new -x509 -nodes -days 2 -sha256 \
    -subj /CN=pg-lake-replication-test-ca \
    -keyout /certs/ca.key -out /certs/ca.crt >/dev/null 2>&1
  openssl req -new -nodes -sha256 -subj /CN=localhost \
    -keyout /certs/server.key -out /certs/server.csr >/dev/null 2>&1
  openssl x509 -req -days 2 -sha256 -in /certs/server.csr \
    -CA /certs/ca.crt -CAkey /certs/ca.key -CAcreateserial \
    -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n') \
    -out /certs/server.crt >/dev/null 2>&1
  openssl req -new -x509 -nodes -days 2 -sha256 \
    -subj /CN=untrusted-test-ca \
    -keyout /certs/untrusted-ca.key -out /certs/untrusted-ca.crt >/dev/null 2>&1
fi

chown -R postgres:postgres /certs
chmod 600 /certs/server.key /certs/ca.key /certs/untrusted-ca.key
chmod 644 /certs/server.crt /certs/ca.crt /certs/untrusted-ca.crt
exec /usr/local/bin/docker-entrypoint.sh "$@"
