#!/bin/bash
set -euo pipefail
mkdir -p /tmp/tls-build/extensions /tmp/tls-certs
cp -a /input/extensions/lake_replication /tmp/tls-build/extensions/
cp -a /input/migrations /tmp/tls-build/
make -s -C /tmp/tls-build/extensions/lake_replication all install
openssl req -new -x509 -nodes -days 1 -subj /CN=test-ca \
  -keyout /tmp/tls-certs/ca.key -out /tmp/tls-certs/ca.crt >/dev/null 2>&1
openssl req -new -nodes -subj /CN=source.test \
  -keyout /tmp/tls-certs/server.key -out /tmp/tls-certs/server.csr >/dev/null 2>&1
printf 'subjectAltName=DNS:source.test\nextendedKeyUsage=serverAuth\n' >/tmp/tls-certs/extensions
openssl x509 -req -days 1 -in /tmp/tls-certs/server.csr -CA /tmp/tls-certs/ca.crt \
  -CAkey /tmp/tls-certs/ca.key -CAcreateserial -extfile /tmp/tls-certs/extensions \
  -out /tmp/tls-certs/server.crt >/dev/null 2>&1
openssl req -new -x509 -nodes -days 1 -subj /CN=wrong-ca \
  -keyout /tmp/tls-certs/wrong.key -out /tmp/tls-certs/wrong.crt >/dev/null 2>&1
chown -R postgres:postgres /tmp/tls-certs
chmod 600 /tmp/tls-certs/*.key
for cluster in tls plain; do
  gosu postgres initdb -D /tmp/$cluster-data --auth=trust --username=postgres \
    --encoding=UTF8 --locale=C >/tmp/$cluster-init.log
  printf 'host all all all trust\n' >>/tmp/$cluster-data/pg_hba.conf
done
gosu postgres pg_ctl -D /tmp/plain-data -l /tmp/plain.log \
  -o '-h 0.0.0.0 -p 5433 -c wal_level=logical' -w start
exec gosu postgres postgres -D /tmp/tls-data -h 0.0.0.0 \
  -c wal_level=logical -c shared_preload_libraries=pg_extension_base \
  -c ssl=on -c ssl_cert_file=/tmp/tls-certs/server.crt -c ssl_key_file=/tmp/tls-certs/server.key
