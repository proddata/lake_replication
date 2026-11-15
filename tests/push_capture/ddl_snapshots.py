#!/usr/bin/env python3
"""Registered source DDL fences must not depend on an older caller snapshot."""
import sys
from live import LiveProbe
from session import Session


class DDLSnapshotProbe(LiveProbe):
    def exercise(self):
        self.evidence.update(scope='source DDL guards under pre-registration repeatable-read snapshots')
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE ROLE application_owner LOGIN;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            ALTER TABLE a OWNER TO application_owner;ALTER TABLE b OWNER TO application_owner''')
        args=self.sql_args();args[args.index('postgres')]='application_owner'
        app=Session(args);private=Session(self.sql_args())
        try:
            app.query('BEGIN ISOLATION LEVEL REPEATABLE READ;SELECT count(*) FROM a')
            private.query('BEGIN ISOLATION LEVEL REPEATABLE READ;SELECT count(*) FROM b')
            self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
            denied=False
            try:
                app.query('ALTER TABLE a ADD COLUMN bypass text;COMMIT')
            except RuntimeError as error:denied='schema is frozen' in str(error)
            self.evidence['application_ddl_committed_without_guard_error']=not denied
            self.check('old application snapshot cannot hide a registered frozen stream',denied,True)
            self.check('rejected old-snapshot application DDL leaves no new column',self.value("SELECT count(*) FROM pg_attribute WHERE attrelid='a'::regclass AND attname='bypass' AND NOT attisdropped"),'0')
            denied=False
            try:
                private.query('ALTER FOREIGN TABLE lake_capture_log.events_a RENAME COLUMN event_hex TO bypass;COMMIT')
            except RuntimeError as error:denied='private schema is frozen' in str(error)
            self.check('old administrator snapshot cannot bypass the private DDL fence',denied,True)
            self.check('rejected private DDL leaves the capture descriptor unchanged',self.value("SELECT count(*) FROM pg_attribute WHERE attrelid='lake_capture_log.events_a'::regclass AND attname='event_hex' AND NOT attisdropped"),'1')
        finally:app.close();private.close()


if __name__=='__main__':sys.exit(DDLSnapshotProbe().run())
