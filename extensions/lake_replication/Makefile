EXTENSION = lake_replication
MODULE_big = lake_replication
OBJS = lake_replication.o
BASELINE_VERSION = 0.1.0
PREVIOUS_VERSION = 0.1.0-beta.1
EXTVERSION = 0.1.0-beta.2
MIGRATIONS = $(sort $(wildcard ../../migrations/[0-9][0-9][0-9][0-9]_*.sql))
EXTENSION_METADATA = extension-config-dump.sql
PREVIOUS_UPGRADES = ../../migrations/0016_observability.sql \
	../../migrations/0017_common_parameterized_types.sql \
	../../migrations/0018_postgres_integration.sql \
	../../migrations/0019_connection_credentials.sql \
	../../migrations/0020_set_based_apply.sql \
	../../migrations/0021_initial_sync_recovery.sql
UPGRADE_MIGRATIONS = ../../migrations/0022_empty_filtered_transactions.sql \
	../../migrations/0023_base_worker_lifecycle.sql
BASELINE_MIGRATIONS = $(filter-out $(PREVIOUS_UPGRADES) $(UPGRADE_MIGRATIONS),$(MIGRATIONS))
PREVIOUS_MIGRATIONS = $(filter-out $(UPGRADE_MIGRATIONS),$(MIGRATIONS))
BASELINE_SQL = $(EXTENSION)--$(BASELINE_VERSION).sql
PREVIOUS_SQL = $(EXTENSION)--$(PREVIOUS_VERSION).sql
CURRENT_SQL = $(EXTENSION)--$(EXTVERSION).sql
PREVIOUS_UPGRADE_SQL = $(EXTENSION)--$(BASELINE_VERSION)--$(PREVIOUS_VERSION).sql
UPGRADE_SQL = $(EXTENSION)--$(PREVIOUS_VERSION)--$(EXTVERSION).sql
BASELINE_UPGRADE_SQL = $(EXTENSION)--$(BASELINE_VERSION)--$(EXTVERSION).sql
DATA_built = $(BASELINE_SQL) $(PREVIOUS_SQL) $(CURRENT_SQL) $(PREVIOUS_UPGRADE_SQL) $(UPGRADE_SQL) $(BASELINE_UPGRADE_SQL)
PG_CONFIG ?= pg_config
PG_CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir)
SHLIB_LINK += -lpq


.PHONY: generate
generate: $(DATA_built)

$(DATA_built): Makefile

$(BASELINE_SQL): build-extension-sql.sh $(BASELINE_MIGRATIONS)
	./build-extension-sql.sh $@ $(BASELINE_MIGRATIONS)

$(PREVIOUS_SQL): build-extension-sql.sh $(PREVIOUS_MIGRATIONS) $(EXTENSION_METADATA)
	./build-extension-sql.sh $@ $(PREVIOUS_MIGRATIONS) $(EXTENSION_METADATA)

$(CURRENT_SQL): build-extension-sql.sh $(MIGRATIONS) $(EXTENSION_METADATA)
	./build-extension-sql.sh $@ $(MIGRATIONS) $(EXTENSION_METADATA)

$(PREVIOUS_UPGRADE_SQL): build-extension-sql.sh $(PREVIOUS_UPGRADES) $(EXTENSION_METADATA)
	./build-extension-sql.sh $@ $(PREVIOUS_UPGRADES) $(EXTENSION_METADATA)

$(UPGRADE_SQL): build-extension-sql.sh $(UPGRADE_MIGRATIONS)
	./build-extension-sql.sh $@ $(UPGRADE_MIGRATIONS)

$(BASELINE_UPGRADE_SQL): build-extension-sql.sh $(PREVIOUS_UPGRADES) $(UPGRADE_MIGRATIONS) $(EXTENSION_METADATA)
	./build-extension-sql.sh $@ $(PREVIOUS_UPGRADES) $(UPGRADE_MIGRATIONS) $(EXTENSION_METADATA)

EXTRA_CLEAN = $(DATA_built)

ifneq ($(filter-out generate,$(MAKECMDGOALS)),)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else ifeq ($(MAKECMDGOALS),)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif
