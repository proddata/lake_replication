"""Diagnose checkpoint lock coupling; a passing reproduction is not a release gate."""


def run(q, c, result):
    database = 'analytics'

    def sql(statement):
        return q.sql('destination', database, statement)

    sid = int(sql("SELECT lake_replication.create_subscription(42,1,1,'checkpoint_probe','checkpoint_probe_slot',ARRAY['pub'])"))
    sql(f"INSERT INTO lake_replication.journal_limits(subscription_id) VALUES({sid})")

    def stage(number):
        return f"SELECT lake_replication.try_stage_transaction({sid},{number},'0/0'::pg_lsn+{number},'0/0'::pg_lsn+{number},'0/0'::pg_lsn+{number},'2026-01-01'::timestamptz,'[]'::jsonb)::text"

    q.check('checkpoint diagnostic stages first transaction', sql(stage(1)), 'true')
    owner = c.Psql('destination', database)
    try:
        owner.query('BEGIN')
        owner.query(f"SELECT lake_replication.prepare_batch({sid},'checkpoint_probe','0/1','0/1')")
        q.check('journal has spare capacity while publisher owns checkpoint',
                sql(f"SELECT (pending_transactions<max_transactions AND pending_payload_bytes<max_payload_bytes)::text FROM lake_replication.journal_admission WHERE subscription_id={sid}"), 'true')
        for name, statement in [
            ('stage', stage(2)),
            ('acknowledgement', f"SELECT lake_replication.mark_acknowledged({sid},'0/1')"),
        ]:
            blocked = q.sql_result('destination', database,
                "SET lock_timeout='250ms'; " + statement, check_result=False)
            q.check(f'publication checkpoint blocks {name} despite free journal capacity',
                    blocked.returncode != 0 and 'lock timeout' in blocked.stderr
                    and 'checkpoints' in blocked.stderr, True)
        owner.query('ROLLBACK')
        q.check('stage resumes after publication checkpoint rollback', sql(stage(2)), 'true')
        q.check('rollback preserves both staged transactions',
                sql(f"SELECT count(*) FROM lake_replication.transactions WHERE subscription_id={sid} AND state='staged'"), '2')
        sql(f"SELECT lake_replication.mark_acknowledged({sid},'0/2')")
        q.check('acknowledgement resumes after checkpoint release',
                sql(f"SELECT acknowledged_lsn FROM lake_replication.checkpoints WHERE subscription_id={sid}"), '0/2')
        result['scope'] = __doc__
        result['finding'] = 'prepare_batch holds the shared checkpoint row until publication ends; staging and acknowledgement serialize behind it even below journal capacity'
    finally:
        owner.close()
