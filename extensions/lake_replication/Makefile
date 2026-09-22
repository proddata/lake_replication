EXTENSION = lake_replication
BASELINE_VERSION = 0.1.0
EXTVERSION = 0.1.0-beta.1
MIGRATIONS = $(sort $(wildcard ../../migrations/[0-9][0-9][0-9][0-9]_*.sql))
EXTENSION_METADATA = extension-config-dump.sql
UPGRADE_SOURCES = ../../migrations/0016_observability.sql $(EXTENSION_METADATA)
BASELINE_MIGRATIONS = $(filter-out ../../migrations/0016_observability.sql,$(MIGRATIONS))
BASELINE_SQL = $(EXTENSION)--$(BASELINE_VERSION).sql
CURRENT_SQL = $(EXTENSION)--$(EXTVERSION).sql
UPGRADE_SQL = $(EXTENSION)--$(BASELINE_VERSION)--$(EXTVERSION).sql
DATA_built = $(BASELINE_SQL) $(CURRENT_SQL) $(UPGRADE_SQL)
PG_CONFIG ?= pg_config

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
