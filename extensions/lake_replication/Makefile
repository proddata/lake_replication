EXTENSION = lake_replication
EXTVERSION = 0.1.0
DATA_built = $(EXTENSION)--$(EXTVERSION).sql
PG_CONFIG ?= pg_config

.PHONY: generate
generate: $(DATA_built)

$(EXTENSION)--$(EXTVERSION).sql: build-extension-sql.sh $(wildcard ../../migrations/*.sql)
	./build-extension-sql.sh $@

EXTRA_CLEAN = $(DATA_built)

ifneq ($(filter-out generate,$(MAKECMDGOALS)),)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else ifeq ($(MAKECMDGOALS),)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif
