#!/usr/bin/env python3
"""Local application transactions, mirror readers and slow publication isolation."""
import sys
import time
from probe import LocalIcebergProbe
from session import Session


class LocalContentionProbe(LocalIcebergProbe):
    def exercise(self):
        self.evidence.update(scope='single-instance open application transaction and mirror-reader contention',
            unqualified=['hard CPU/RAM admission limits','production load','all lock graphs'])
        self.setup()
        application=Session(self.sql_args());reader=Session(self.sql_args())
        try:
            application.query("BEGIN;UPDATE a SET note='committed later' WHERE id=1")
            self.sql("INSERT INTO b VALUES(3,'independent commit')")
            self.check('independent committed table publishes while application transaction remains open',self.wait("SELECT note FROM iceberg_mirror.b WHERE id=3",'independent commit',timeout=20),'independent commit')
            self.check('uncommitted application mutation is not visible in Iceberg',self.value('SELECT note FROM iceberg_mirror.a WHERE id=1'),'baseline')
            application.query('COMMIT')
            self.wait("SELECT note FROM iceberg_mirror.a WHERE id=1",'committed later');self.equal()
            reader.query("BEGIN ISOLATION LEVEL REPEATABLE READ;SELECT note FROM iceberg_mirror.a WHERE id=1")
            self.sql("UPDATE a SET note='new published version' WHERE id=1")
            self.check('ordinary mirror reader does not block publication',self.wait("SELECT note FROM iceberg_mirror.a WHERE id=1",'new published version',timeout=20),'new published version')
            self.check('repeatable-read mirror reader retains its old committed version',reader.query('SELECT note FROM iceberg_mirror.a WHERE id=1'),['committed later'])
            reader.query('COMMIT')
            self.check('new reader snapshot observes the latest Iceberg version',reader.query('SELECT note FROM iceberg_mirror.a WHERE id=1'),['new published version'])
            self.equal()
            # Hold a lock on the local mirror rather than globally blocking the
            # object-store service. Appending to the ordinary heaps must proceed.
            reader.query('BEGIN;ALTER FOREIGN TABLE iceberg_mirror.a OWNER TO local_worker')
            self.sql("SET statement_timeout='3s';BEGIN;INSERT INTO a VALUES(4,'queued during mirror lock');INSERT INTO b VALUES(4,'same transaction');COMMIT")
            self.check('blocked mirror publication does not block application commit',self.value("SELECT note FROM a WHERE id=4"),'queued during mirror lock')
            self.check('publisher actually waits on the held mirror relation lock',self.wait("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE usename='local_worker' AND wait_event_type='Lock')::text",'true'),'true')
            self.check('receiver journals the committed application transaction while publisher waits',self.wait("SELECT EXISTS(SELECT FROM lake_replication.transactions t JOIN lake_replication.subscriptions s USING(subscription_id) WHERE s.subscription_name='local_iceberg')::text",'true'),'true')
            self.sql("SET statement_timeout='3s';INSERT INTO b VALUES(5,'another committed writer')")
            reader.query('COMMIT')
            self.wait("SELECT note FROM iceberg_mirror.a WHERE id=4",'queued during mirror lock');self.wait("SELECT note FROM iceberg_mirror.b WHERE id=5",'another committed writer');self.equal()
        finally:
            application.close();reader.close()
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.check('contention recovery leaves no capture slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0'),'0')


if __name__=='__main__':sys.exit(LocalContentionProbe().run())
