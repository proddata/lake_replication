#!/usr/bin/env python3
"""Historical identities, TOAST, ordered mutations and TRUNCATE end to end."""
import json
import sys
from live import LiveProbe


class MutationProbe(LiveProbe):
    def exercise(self):
        self.evidence.update(scope='native frozen-schema mutations and historical TOAST',
            unqualified=['online snapshot','DDL capture','scoped source roles','physical cleanup','production API'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint,part integer,active boolean,note text,
                PRIMARY KEY(id,part) INCLUDE(active));
            CREATE TABLE b("key's name" text PRIMARY KEY,note text);
            ALTER TABLE a ALTER COLUMN note SET STORAGE EXTERNAL;
            INSERT INTO a VALUES(1,1,true,repeat('external-toast-',80000)),(2,2,false,NULL);
            INSERT INTO b VALUES('old','original');''')
        # A different replica identity index omits the primary key in old rows.
        self.sql('CREATE UNIQUE INDEX alternative_identity ON b(note);ALTER TABLE b REPLICA IDENTITY USING INDEX alternative_identity',required=False)
        # PostgreSQL itself requires NOT NULL for identity indexes; test with a
        # separate supported scalar so the restriction is exercised reliably.
        self.sql('ALTER TABLE b ADD COLUMN other bigint NOT NULL DEFAULT 7;CREATE UNIQUE INDEX other_identity ON b(other);ALTER TABLE b REPLICA IDENTITY USING INDEX other_identity')
        denied=self.sql("SELECT lake_capture.register_stream('a','b',NULL,'paused_copy')",required=False)
        self.check('registration rejects an identity index that omits the primary key',denied.returncode!=0,True)
        self.sql('ALTER TABLE b REPLICA IDENTITY DEFAULT;DROP INDEX other_identity;ALTER TABLE b DROP COLUMN other')
        # Dropped columns are deliberately unsupported by the frozen schema.
        self.sql('DROP TABLE b;CREATE TABLE b("key\'s name" text PRIMARY KEY,note text)')
        self.sql("INSERT INTO b VALUES('old','original')")
        self.stream=self.value("SELECT lake_capture.register_stream('a','b',NULL,'paused_copy')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait("SELECT snapshot_state FROM lake_capture.streams",'complete')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination()
        self.sequence=1
        self.publish('initial external-TOAST snapshot')
        self.change('''BEGIN;
            UPDATE a SET active=false WHERE id=1;
            UPDATE a SET id=10,part=11 WHERE id=1;
            UPDATE a SET note='inline' WHERE id=2;
            UPDATE b SET "key's name"='new',note=NULL WHERE "key's name"='old';
            COMMIT;''','unchanged historical TOAST and compound/quoted key changes')
        self.check('native decoder encodes unchanged TOAST instead of fetching current rows',self.value('''SELECT EXISTS(
            SELECT FROM lake_capture_log.events_a WHERE sequence=2 AND position('0000000402' in event_hex)>0)'''),'t')
        self.change('''BEGIN;
            DELETE FROM a WHERE id=2;INSERT INTO a VALUES(2,2,true,repeat('new-toast-',90000));
            UPDATE a SET note=NULL WHERE id=10;
            UPDATE a SET active=true WHERE id=2;
            DELETE FROM b WHERE "key's name"='new';INSERT INTO b VALUES('new','reinsert');
            SAVEPOINT aborted;UPDATE a SET id=999;ROLLBACK TO aborted;
            COMMIT;''','delete/reinsert, repeated updates, NULL and external transitions')
        self.change('''BEGIN;
            UPDATE a SET note='before-truncate';TRUNCATE a,b RESTART IDENTITY;
            INSERT INTO a VALUES(3,4,true,'after-truncate');INSERT INTO b VALUES('after','barrier');
            COMMIT;''','two-table TRUNCATE barrier followed by same-transaction INSERT')
        self.change('''BEGIN;DELETE FROM a;DELETE FROM b;COMMIT;''','DELETE leaves exact empty mirrors')
        before=self.dest('SELECT count(*) FROM mirror.a')
        self.importer(LAKE_PUSH_REPLAY_FROM=str(self.sequence))
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('identical mutation replay does not reapply destructive operations',self.dest('SELECT count(*) FROM mirror.a'),before)

    def change(self,statement,label):
        self.sql(statement);self.sequence+=1
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams',str(self.sequence))
        self.publish(label)

    def publish(self,label):
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        for name in 'ab':
            expected=self.value(f"SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM {name} t")
            self.check(label+' '+name,self.dest(f"SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]') FROM mirror.{name} t"),expected)


if __name__=='__main__':sys.exit(MutationProbe().run())
