#!/usr/bin/env python3
"""Native local-catalog feasibility for ordered destination DML/DDL commits."""
import sys
from live import LiveProbe


class SchemaFeasibilityProbe(LiveProbe):
    def exercise(self):
        self.evidence.update(scope='destination pg_lake mixed DML/DDL transaction feasibility',
            unqualified=['source schema capture/audit','schema journal admission','ordered publisher integration',
                         'source historical schema descriptors','REST catalogs','production schema evolution'])
        self.dest('''CREATE SCHEMA schema_probe;
            CREATE TABLE schema_probe.a(id bigint,note text) USING iceberg;
            CREATE TABLE schema_probe.b(id bigint,note text) USING iceberg;
            CREATE TABLE schema_probe.checkpoint(version integer);INSERT INTO schema_probe.checkpoint VALUES(0)''')
        self.dest('''BEGIN;
            INSERT INTO schema_probe.a VALUES(1,'old');INSERT INTO schema_probe.b VALUES(1,'old');
            ALTER TABLE schema_probe.a ADD COLUMN extra text;
            ALTER TABLE schema_probe.b ADD COLUMN extra text;
            INSERT INTO schema_probe.a VALUES(2,'new','added');UPDATE schema_probe.b SET extra='after' WHERE id=1;
            UPDATE schema_probe.checkpoint SET version=1;COMMIT''')
        self.check('old-schema and new-schema inserts share one committed table',self.dest('''SELECT
            (count(*)=2 AND count(*) FILTER(WHERE id=1 AND note='old' AND extra IS NULL)=1
                AND count(*) FILTER(WHERE id=2 AND note='new' AND extra='added')=1)::text FROM schema_probe.a'''),'true')
        self.check('second table publishes the same additive-schema transaction',self.dest(
            "SELECT (count(*)=1 AND bool_and(id=1 AND extra='after'))::text FROM schema_probe.b"),'true')
        self.check('added fields are nullable scalar columns',self.dest('''SELECT (count(*)=2 AND bool_and(
            atttypid='text'::regtype AND NOT attnotnull))::text FROM pg_attribute
            WHERE attrelid IN ('schema_probe.a'::regclass,'schema_probe.b'::regclass) AND attname='extra' '''),'true')
        self.check('heap checkpoint commits with both additive table changes',self.dest('SELECT version FROM schema_probe.checkpoint'),'1')
        self.dest('''BEGIN;UPDATE schema_probe.a SET note='before rename' WHERE id=1;
            ALTER TABLE schema_probe.a RENAME COLUMN note TO detail;
            INSERT INTO schema_probe.a VALUES(3,'after rename','third');
            UPDATE schema_probe.checkpoint SET version=2;COMMIT''')
        self.check('rename preserves writes on each side of its transaction boundary',self.dest('''SELECT
            (count(*)=3 AND count(*) FILTER(WHERE id=1 AND detail='before rename')=1
                AND count(*) FILTER(WHERE id=2 AND detail='new')=1
                AND count(*) FILTER(WHERE id=3 AND detail='after rename')=1)::text FROM schema_probe.a'''),'true')
        self.check('committed rename updates the visible descriptor',self.dest('''SELECT
            count(*) FILTER(WHERE attname='detail')::text||':'||count(*) FILTER(WHERE attname='note')
            FROM pg_attribute WHERE attrelid='schema_probe.a'::regclass AND attnum>0 AND NOT attisdropped'''),'1:0')
        failed=self.destination('''BEGIN;UPDATE schema_probe.a SET detail='uncommitted';
            ALTER TABLE schema_probe.a ADD COLUMN rolled_back boolean;
            INSERT INTO schema_probe.b VALUES(2,'discarded',NULL);
            UPDATE schema_probe.checkpoint SET version=3;SELECT 1/0;COMMIT''',required=False)
        self.check('late failure aborts a mixed schema/data transaction',failed.returncode!=0 and 'division by zero' in failed.stderr,True)
        self.check('failed nullable addition leaves no visible column',self.dest('''SELECT count(*) FROM pg_attribute
            WHERE attrelid='schema_probe.a'::regclass AND attname='rolled_back' AND NOT attisdropped'''),'0')
        self.check('failed mixed transaction restores both table contents',self.dest('''SELECT
            ((SELECT count(*) FROM schema_probe.a WHERE detail='uncommitted')=0
                AND (SELECT count(*) FROM schema_probe.b)=1)::text'''),'true')
        self.check('failed mixed transaction cannot advance the checkpoint',self.dest('SELECT version FROM schema_probe.checkpoint'),'2')
        failed=self.destination('''BEGIN;ALTER TABLE schema_probe.b RENAME COLUMN note TO discarded_name;
            INSERT INTO schema_probe.a VALUES(4,'discarded','discarded');
            UPDATE schema_probe.checkpoint SET version=3;SELECT 1/0;COMMIT''',required=False)
        self.check('late failure aborts a rename and following write',failed.returncode!=0,True)
        self.check('failed rename restores old names and rejects its later row',self.dest('''SELECT
            ((SELECT count(*) FROM pg_attribute WHERE attrelid='schema_probe.b'::regclass
                AND attname='note' AND NOT attisdropped)=1
                AND NOT EXISTS(SELECT FROM schema_probe.a WHERE id=4))::text'''),'true')
        self.dest('''BEGIN;ALTER TABLE schema_probe.a ADD COLUMN flag boolean;
            ALTER TABLE schema_probe.b RENAME COLUMN extra TO details;
            UPDATE schema_probe.checkpoint SET version=3;COMMIT''')
        self.check('DDL-only transaction commits schema and checkpoint together',self.dest('''SELECT
            ((SELECT count(*) FROM schema_probe.a WHERE flag IS NULL)=3
                AND (SELECT details FROM schema_probe.b WHERE id=1)='after'
                AND (SELECT version FROM schema_probe.checkpoint)=3)::text'''),'true')


if __name__=='__main__':sys.exit(SchemaFeasibilityProbe().run())
