COMPOSE = docker compose -p pg-lake-replication-lab -f deploy/docker-compose/compose.yml
FAILOVER_COMPOSE = docker compose -p pg-lake-replication-failover-lab -f deploy/failover/compose.yml
PATRONI_COMPOSE = docker compose -p pg-lake-replication-patroni-lab -f deploy/patroni/compose.yml
SLOT_LOSS_COMPOSE = docker compose -p pg-lake-replication-slot-loss-lab -f deploy/slot-loss/compose.yml
SCRAM_COMPOSE = docker compose -p pg-lake-replication-scram-lab -f deploy/scram/compose.yml
TLS_COMPOSE = docker compose -p pg-lake-replication-tls-lab -f deploy/tls/compose.yml

.PHONY: lab-build lab-up lab-test lab-crash-test lab-logs lab-stop failover-up failover-test failover-stop patroni-up patroni-test patroni-stop slot-loss-up slot-loss-test slot-loss-stop scram-test tls-test journal-install journal-test journal-maintenance-test transport-test publisher-test mutations-test streaming-test toast-test initial-sync-test multi-table-test auto-schema-test schema-evolution-test rebuild-test supervisor-test source-continuity-test source-endpoints-test check
lab-build:
	$(COMPOSE) build
lab-up:
	$(COMPOSE) up -d --wait --wait-timeout 180
lab-test:
	python3 tests/replication/probe.py

failover-up:
	$(FAILOVER_COMPOSE) up -d --wait --wait-timeout 180
failover-test:
	python3 tests/failover/probe.py
failover-stop:
	$(FAILOVER_COMPOSE) down -v

patroni-up:
	$(PATRONI_COMPOSE) up -d --build --wait --wait-timeout 240
patroni-test:
	python3 tests/patroni/probe.py
patroni-stop:
	$(PATRONI_COMPOSE) down -v

slot-loss-up:
	$(SLOT_LOSS_COMPOSE) up -d --wait --wait-timeout 180
slot-loss-test:
	python3 tests/slot_loss/probe.py
slot-loss-stop:
	$(SLOT_LOSS_COMPOSE) down -v

scram-test:
	python3 tests/scram/probe.py
tls-test:
	python3 tests/tls/probe.py

lab-crash-test:
	python3 tests/crash/probe.py

journal-install:
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0001_journal.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0002_transport.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0003_publisher.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0004_mutations.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0005_streaming.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0006_initial_sync.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0007_multi_table_sync.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0008_type_contracts.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0009_schema_evolution.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0010_rebuild_promotion.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0011_supervisor.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0012_service_reconnect.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0013_source_continuity.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0014_source_endpoints.sql
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -f - < migrations/0015_journal_maintenance.sql

journal-test:
	python3 tests/journal/probe.py

journal-maintenance-test:
	python3 tests/journal_maintenance/probe.py

transport-test:
	python3 tests/transport/probe.py

publisher-test:
	python3 tests/publisher/probe.py

mutations-test:
	python3 tests/mutations/probe.py

streaming-test:
	python3 tests/streaming/probe.py

toast-test:
	python3 tests/toast/probe.py
initial-sync-test:
	python3 tests/initial_sync/probe.py
multi-table-test:
	python3 tests/multi_table/probe.py
auto-schema-test:
	python3 tests/auto_schema/probe.py
schema-evolution-test:
	python3 tests/schema_evolution/probe.py
rebuild-test:
	python3 tests/rebuild/probe.py
supervisor-test:
	python3 tests/supervisor/probe.py
source-continuity-test:
	python3 tests/source_continuity/probe.py
source-endpoints-test:
	python3 tests/source_endpoints/probe.py
lab-logs:
	$(COMPOSE) logs --no-color --tail 200
lab-stop:
	$(COMPOSE) stop
check:
	python3 -m py_compile tests/replication/probe.py
	python3 -m py_compile tests/crash/probe.py
	python3 -m py_compile tests/journal/probe.py
	python3 -m py_compile tests/transport/probe.py
	python3 -m py_compile tests/publisher/probe.py
	python3 -m py_compile tests/mutations/probe.py
	python3 -m py_compile tests/streaming/probe.py
	python3 -m py_compile tests/toast/probe.py
	python3 -m py_compile tests/initial_sync/probe.py
	python3 -m py_compile tests/multi_table/probe.py
	python3 -m py_compile tests/auto_schema/probe.py
	python3 -m py_compile tests/schema_evolution/probe.py
	python3 -m py_compile tests/rebuild/probe.py
	python3 -m py_compile tests/supervisor/probe.py
	python3 -m py_compile tests/source_continuity/probe.py
	python3 -m py_compile tests/source_endpoints/probe.py
	python3 -m py_compile tests/failover/probe.py
	python3 -m py_compile tests/patroni/probe.py
	python3 -m py_compile tests/slot_loss/probe.py
	python3 -m py_compile tests/scram/probe.py
	python3 -m py_compile tests/tls/probe.py
	python3 -m py_compile tests/journal_maintenance/probe.py
	python3 -m unittest discover -s tests/replication -p 'test_*.py'
	cargo fmt --all -- --check
	cargo test --workspace
	$(COMPOSE) config --quiet
	$(FAILOVER_COMPOSE) config --quiet
	$(PATRONI_COMPOSE) config --quiet
	$(SLOT_LOSS_COMPOSE) config --quiet
	AUTH_LAB_ADMIN_PASSWORD=check AUTH_LAB_SOURCE_PASSWORD=check $(SCRAM_COMPOSE) config --quiet
	TLS_LAB_ADMIN_PASSWORD=check TLS_LAB_SOURCE_PASSWORD=check $(TLS_COMPOSE) config --quiet
