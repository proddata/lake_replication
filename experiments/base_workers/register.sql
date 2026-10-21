-- Experimental registration, appended only inside the disposable image.
CREATE FUNCTION lake_replication.prototype_launcher(internal)
RETURNS internal LANGUAGE C
AS 'MODULE_PATHNAME', 'lake_replication_prototype_launcher';
SELECT extension_base.register_worker(
    'lake_replication prototype launcher', 'lake_replication.prototype_launcher');
