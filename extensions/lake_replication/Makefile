EXTENSION = lake_replication
EXTVERSION = 0.1.0
DATA_built = $(EXTENSION)--$(EXTVERSION).sql
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

$(EXTENSION)--$(EXTVERSION).sql: build-extension-sql.sh $(wildcard ../../migrations/*.sql)
	./build-extension-sql.sh $@

EXTRA_CLEAN = $(DATA_built)

include $(PGXS)
