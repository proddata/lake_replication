EXTENSION = lake_replication_capture
MODULE_big = lake_replication_capture
OBJS = decode.o launcher.o schema.o
DATA = lake_replication_capture--0.1.0.sql
PG_CONFIG ?= pg_config
PG_EXTENSION_BASE_INCLUDE ?= $(shell $(PG_CONFIG) --includedir-server)/extension
PG_CPPFLAGS += -I$(PG_EXTENSION_BASE_INCLUDE)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
