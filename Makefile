COMPOSE = docker compose -p lake-replication-lab -f deploy/docker-compose/compose.yml
FAILOVER_COMPOSE = docker compose -p lake-replication-failover-lab -f deploy/failover/compose.yml
PATRONI_COMPOSE = docker compose -p lake-replication-patroni-lab -f deploy/patroni/compose.yml
SLOT_LOSS_COMPOSE = docker compose -p lake-replication-slot-loss-lab -f deploy/slot-loss/compose.yml
SCRAM_COMPOSE = docker compose -p lake-replication-scram-lab -f deploy/scram/compose.yml
TLS_COMPOSE = docker compose -p lake-replication-tls-lab -f deploy/tls/compose.yml
POC_COMPOSE = docker compose -p lake-replication-poc -f deploy/poc/compose.yml
QUALIFICATION_COMPOSE = docker compose -f deploy/qualification/compose.yml
PG_LAKE_REV ?= $(shell git -C vendor/pg_lake rev-parse HEAD)
.DEFAULT_GOAL := lab-build

.PHONY: upstream-source

.PHONY: lab-build lab-up lab-test lab-crash-test lab-logs lab-stop performance-test failover-up failover-test failover-stop patroni-up patroni-test patroni-stop slot-loss-up slot-loss-test slot-loss-stop scram-test tls-test journal-install journal-test journal-maintenance-test transport-test publisher-test mutations-test streaming-test empty-filtered-test receiver-progress-test toast-test initial-sync-test multi-table-test auto-schema-test schema-evolution-test rebuild-test supervisor-test source-continuity-test source-endpoints-test iceberg-maintenance-test native-subscription-spike-test postgres-integration-test lake-replication-extension lake-replication-extension-test install-bundle install-bundles install-bundle-test qualification-test compatibility-test poc-build poc-up poc-logs poc-stop poc-down check
upstream-source:
	python3 deploy/prepare-upstream.py $(PG_LAKE_REV)

lab-build: upstream-source
	PG_LAKE_REV=$(PG_LAKE_REV) $(COMPOSE) build
lab-up: upstream-source
	PG_LAKE_REV=$(PG_LAKE_REV) $(COMPOSE) up -d --wait --wait-timeout 180
lab-test:
	python3 tests/replication/probe.py

performance-test:
	python3 tests/performance/probe.py

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
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d analytics -c 'CREATE EXTENSION IF NOT EXISTS dblink; CREATE EXTENSION IF NOT EXISTS lake_replication;'

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

receiver-progress-test:
	$(MAKE) -C extensions/lake_replication generate
	python3 tests/streaming/empty_filtered.py --current-workers --native-bulk

empty-filtered-test:
	$(MAKE) -C extensions/lake_replication generate
	python3 tests/streaming/empty_filtered.py

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
iceberg-maintenance-test:
	python3 tests/iceberg_maintenance/probe.py
native-subscription-spike-test:
	python3 tests/native_subscription_spike/probe.py
postgres-integration-test:
	python3 tests/postgres_integration/probe.py
lake-replication-extension:
	$(MAKE) -C extensions/lake_replication generate
lake-replication-extension-test:
	python3 tests/extension/probe.py
install-bundle:
	./deploy/install/build-bundle.sh
install-bundles:
	./deploy/install/build-release.sh
install-bundle-test:
	python3 tests/install/probe.py
qualification-test:
	python3 tests/qualification/probe.py
compatibility-test:
	python3 tests/qualification/compatibility.py
lab-logs:
	$(COMPOSE) logs --no-color --tail 200
lab-stop:
	$(COMPOSE) stop

poc-build: upstream-source
	PG_LAKE_REV=$(PG_LAKE_REV) $(POC_COMPOSE) build
poc-up: upstream-source
	PG_LAKE_REV=$(PG_LAKE_REV) $(POC_COMPOSE) up -d --wait --wait-timeout 180
	@echo "Demo console: http://127.0.0.1:8080"
poc-logs:
	$(POC_COMPOSE) logs --no-color --tail 200 -f
poc-stop:
	$(POC_COMPOSE) stop
poc-down:
	$(POC_COMPOSE) down -v
check:
	python3 -m py_compile deploy/prepare-upstream.py
	python3 -m py_compile experiments/base_workers/adapt.py experiments/base_workers/probe.py experiments/base_workers/compare.py experiments/base_workers/sample.py experiments/base_workers/analyze.py experiments/base_workers/upgrade.py experiments/base_workers/faults.py experiments/base_workers/recovery.py experiments/base_workers/identity.py experiments/base_workers/binding.py experiments/base_workers/rebuild.py experiments/base_workers/publication.py experiments/base_workers/ownership.py experiments/base_workers/transfer.py experiments/base_workers/scale.py experiments/base_workers/admission.py experiments/base_workers/admission_probe.py experiments/base_workers/admission_measure.py
	python3 -m unittest discover -s experiments/base_workers -p 'test_*.py'
	python3 -m py_compile tests/replication/probe.py
	python3 -m py_compile tests/performance/probe.py
	python3 -m py_compile tests/crash/probe.py
	python3 -m py_compile tests/journal/probe.py
	python3 -m py_compile tests/transport/probe.py
	python3 -m py_compile tests/publisher/probe.py
	python3 -m py_compile tests/mutations/probe.py
	python3 -m py_compile tests/streaming/probe.py tests/streaming/empty_filtered.py
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
	python3 -m py_compile tests/iceberg_maintenance/probe.py
	python3 -m py_compile tests/native_subscription_spike/probe.py
	python3 -m py_compile tests/postgres_integration/probe.py
	python3 -m py_compile tests/extension/probe.py
	python3 -m py_compile tests/install/probe.py
	python3 -m py_compile tests/qualification/probe.py
	python3 -m py_compile tests/qualification/compatibility.py
	$(MAKE) -C extensions/lake_replication generate
	! grep -q '0016_observability.sql' extensions/lake_replication/lake_replication--0.1.0.sql
	! grep -q '0017_common_parameterized_types.sql' extensions/lake_replication/lake_replication--0.1.0.sql
	! grep -q '0018_postgres_integration.sql' extensions/lake_replication/lake_replication--0.1.0.sql
	! grep -q '0019_connection_credentials.sql' extensions/lake_replication/lake_replication--0.1.0.sql
	! grep -q '0022_empty_filtered_transactions.sql' extensions/lake_replication/lake_replication--0.1.0.sql
	! grep -q '0020_set_based_apply.sql' extensions/lake_replication/lake_replication--0.1.0.sql
	grep -q '0016_observability.sql' extensions/lake_replication/lake_replication--0.1.0--0.1.0-beta.1.sql
	grep -q '0017_common_parameterized_types.sql' extensions/lake_replication/lake_replication--0.1.0--0.1.0-beta.1.sql
	grep -q '0018_postgres_integration.sql' extensions/lake_replication/lake_replication--0.1.0--0.1.0-beta.1.sql
	grep -q '0019_connection_credentials.sql' extensions/lake_replication/lake_replication--0.1.0--0.1.0-beta.1.sql
	grep -q '0022_empty_filtered_transactions.sql' extensions/lake_replication/lake_replication--0.1.0--0.1.0-beta.1.sql
	grep -q '0020_set_based_apply.sql' extensions/lake_replication/lake_replication--0.1.0--0.1.0-beta.1.sql
	grep -q 'CREATE OR REPLACE FUNCTION lake_replication.register_type_contract' extensions/lake_replication/lake_replication--0.1.0--0.1.0-beta.1.sql
	grep -q 'CREATE OR REPLACE FUNCTION lake_replication.ensure_iceberg_destination' extensions/lake_replication/lake_replication--0.1.0--0.1.0-beta.1.sql
	grep -q 'CREATE OR REPLACE FUNCTION lake_replication.reconcile_relation_schema' extensions/lake_replication/lake_replication--0.1.0--0.1.0-beta.1.sql
	grep -q 'pg_extension_config_dump' extensions/lake_replication/lake_replication--0.1.0-beta.1.sql
	grep -q 'CREATE TABLE lake_replication.worker_launch_config' extensions/lake_replication/lake_replication--0.1.0-beta.1.sql
	grep -q "LOAD 'lake_replication'" extensions/lake_replication/lake_replication--0.1.0--0.1.0-beta.1.sql
	python3 -m unittest discover -s tests/replication -p 'test_*.py'
	cargo fmt --all -- --check
	cargo test --workspace
	$(COMPOSE) config --quiet
	$(FAILOVER_COMPOSE) config --quiet
	$(PATRONI_COMPOSE) config --quiet
	$(SLOT_LOSS_COMPOSE) config --quiet
	AUTH_LAB_ADMIN_PASSWORD=check AUTH_LAB_SOURCE_PASSWORD=check $(SCRAM_COMPOSE) config --quiet
	TLS_LAB_ADMIN_PASSWORD=check TLS_LAB_SOURCE_PASSWORD=check $(TLS_COMPOSE) config --quiet
	$(POC_COMPOSE) config --quiet
	QUALIFICATION_SOURCE_IMAGE=check QUALIFICATION_DESTINATION_IMAGE=check QUAL_ADMIN_PASSWORD=check QUAL_SOURCE_PASSWORD=check QUAL_ROTATED_SOURCE_PASSWORD=check-rotated $(QUALIFICATION_COMPOSE) config --quiet
