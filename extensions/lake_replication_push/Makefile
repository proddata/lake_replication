EXTENSION = lake_replication_push
MODULE_big = lake_replication_push
OBJS = guard.o
DATA_built = lake_replication_push--0.1.0.sql
EXTRA_CLEAN = $(DATA_built)
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
lake_replication_push--0.1.0.sql: schema.sql schema_codec.sql schema_history.sql schema_admission.sql schema_publish.sql api.sql
	cat $^ > $@
