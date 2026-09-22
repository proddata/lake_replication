MODULES = lake_subscription_sugar
EXTENSION = lake_subscription_sugar
DATA = lake_subscription_sugar--1.0.sql lake_subscription_sugar--1.0--1.1.sql
DATA_built = lake_subscription_sugar--1.1.sql
EXTRA_CLEAN = $(DATA_built)
PG_CONFIG = pg_config

lake_subscription_sugar--1.1.sql: lake_subscription_sugar--1.0.sql build-extension-sql.sh
	./build-extension-sql.sh $@

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
