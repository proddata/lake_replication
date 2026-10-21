EXTENSION = lake_replication
MODULE_big = lake_replication
OBJS = lake_replication.o
BASELINE_VERSION = 0.1.0
EXTVERSION = 0.1.0-beta.1
MIGRATIONS = $(sort $(wildcard ../../migrations/[0-9][0-9][0-9][0-9]_*.sql))
EXTENSION_METADATA = extension-config-dump.sql
UPGRADE_MIGRATIONS = ../../migrations/0016_observability.sql \
	../../migrations/0017_common_parameterized_types.sql \
	../../migrations/0018_postgres_integration.sql \
	../../migrations/0019_connection_credentials.sql \
	../../migrations/0020_set_based_apply.sql \
	../../migrations/0021_initial_sync_recovery.sql \
	../../migrations/0022_empty_filtered_transactions.sql
UPGRADE_SOURCES = $(UPGRADE_MIGRATIONS) $(EXTENSION_METADATA)
BASELINE_MIGRATIONS = $(filter-out $(UPGRADE_MIGRATIONS),$(MIGRATIONS))
BASELINE_SQL = $(EXTENSION)--$(BASELINE_VERSION).sql
CURRENT_SQL = $(EXTENSION)--$(EXTVERSION).sql
UPGRADE_SQL = $(EXTENSION)--$(BASELINE_VERSION)--$(EXTVERSION).sql
DATA_built = $(BASELINE_SQL) $(CURRENT_SQL) $(UPGRADE_SQL)
PG_CONFIG ?= pg_config
PG_CPPFLAGS += -I$(shell $(PG_CONFIG) --includedir)
SHLIB_LINK += -lpq

.PHONY: generate
generate: $(DATA_built)

$(BASELINE_SQL): build-extension-sql.sh $(BASELINE_MIGRATIONS)
	./build-extension-sql.sh $@ $(BASELINE_MIGRATIONS)

$(CURRENT_SQL): build-extension-sql.sh $(MIGRATIONS) $(EXTENSION_METADATA)
	./build-extension-sql.sh $@ $(MIGRATIONS) $(EXTENSION_METADATA)

$(UPGRADE_SQL): build-extension-sql.sh $(UPGRADE_SOURCES)
	./build-extension-sql.sh $@ $(UPGRADE_SOURCES)

EXTRA_CLEAN = $(DATA_built)

ifneq ($(filter-out generate,$(MAKECMDGOALS)),)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else ifeq ($(MAKECMDGOALS),)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif
