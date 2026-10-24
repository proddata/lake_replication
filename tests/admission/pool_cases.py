"""Shared pool behavior cases; callers supply q/c and the SQL helper/view names."""
from concurrent.futures import ThreadPoolExecutor
import time

q = c = None
VIEW = 'lake_replication.stream_admission'
STREAM = 'SELECT lake_replication.admit_stream()'
CLIENTS = []


def client(database='analytics'):
    connection=c.Psql('destination',database);CLIENTS.append(connection)
    return connection


def sql(statement):return q.sql('destination','analytics',statement)


def count(state=None):
    return int(sql(f"SELECT count(*) FROM {VIEW}"+(f" WHERE admission_state='{state}'" if state else '')))


def wait_count(state,expected):
    q.check(f'{state} reaches {expected}',q.wait_for('destination','analytics',
        f"SELECT count(*) FROM {VIEW} WHERE admission_state='{state}'",str(expected)),str(expected))


def holders():
    connections=[client('analytics' if i%2==0 else 'pool_other') for i in range(4)]
    for connection in connections:connection.query('BEGIN; '+STREAM)
    wait_count('admitted',4)
    return connections


def aborts(observed):
    q.sql('destination','analytics','CREATE DATABASE pool_other')
    q.sql('destination','pool_other','CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
    owner=client();owner.query('BEGIN; SAVEPOINT work; '+STREAM+'; '+STREAM)
    q.check('nested calls use one slot',count('admitted'),1)
    owner.query('ROLLBACK TO work');q.check('savepoint releases a newly acquired slot',count(),0)
    owner.query(STREAM);owner.query('SAVEPOINT nested; '+STREAM+'; ROLLBACK TO nested')
    q.check('nested rollback preserves outer slot',count('admitted'),1)
    owner.query('ROLLBACK')
    active=holders()
    q.check('pool spans both destination databases',sql(f'SELECT count(DISTINCT database_name) FROM {VIEW}'),'2')
    rejected=q.sql_result('destination','pool_other',"SET lock_timeout='250ms'; "+STREAM,check_result=False)
    q.check('cross-database timeout propagates',rejected.returncode!=0 and 'lock timeout' in rejected.stderr,True)
    q.check('timeout leaves four holders and no waiter',(count('admitted'),count('waiting_for_stream')),(4,0))
    waiting=client('pool_other');waiting.query('BEGIN');pid=waiting.query('SELECT pg_backend_pid()')[0]
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending=pool.submit(waiting.query,STREAM);wait_count('waiting_for_stream',1)
        sql(f'SELECT pg_cancel_backend({pid})')
        try:pending.result(timeout=10)
        except RuntimeError as error:
            waiting.error_reader.join(timeout=2)
            q.check('cross-database cancellation propagates',
                    'canceling statement' in (str(error)+''.join(waiting.errors)),True)
        else:raise AssertionError('waiting stream survived cancellation')
    q.check('cancellation clears only its waiter',(count('admitted'),count('waiting_for_stream')),(4,0))
    for connection in active:connection.query('ROLLBACK')
    victim=client('pool_other');victim.query('BEGIN; '+STREAM);pid=victim.query('SELECT pg_backend_pid()')[0]
    sql(f'SELECT pg_terminate_backend({pid})');wait_count('admitted',0)
    q.check('backend death releases the final slot',count(),0)


def progress(observed):
    active=holders();waiting=[client('pool_other') for _ in range(8)]
    pool=ThreadPoolExecutor(max_workers=8);pending={}
    completed=[];maximum=4
    try:
        for connection in waiting:
            connection.query('BEGIN');pid=connection.query('SELECT pg_backend_pid()')[0]
            pending[pool.submit(connection.query,STREAM)]=(connection,pid)
        wait_count('waiting_for_stream',8)
        for connection in active:connection.query('ROLLBACK')
        deadline=time.monotonic()+20
        while pending and time.monotonic()<deadline:
            maximum=max(maximum,count('admitted'))
            ready=[future for future in pending if future.done()]
            for future in ready:
                connection,pid=pending.pop(future);future.result(timeout=1)
                completed.append(pid);connection.query('ROLLBACK')
            if not ready:time.sleep(.02)
        q.check('every queued cross-database transaction makes progress',len(completed),8)
        q.check('observed pool occupancy never exceeds four',maximum<=4,True)
        q.check('busy pool drains after all queued transactions',count(),0)
        observed.update(completed_waiters=len(completed),maximum_observed_holders=maximum)
    finally:
        # Release holders before joining a waiter if an assertion failed.
        for connection in active+waiting:connection.close()
        pool.shutdown(wait=True,cancel_futures=True)


def mixed(observed):
    bulk=client();bulk.query("SET deadlock_timeout='10s'; BEGIN; SELECT lake_replication.admit_bulk()")
    bulk_pid=int(bulk.query('SELECT pg_backend_pid()')[0])
    active=holders()
    chosen_slot=bulk_pid%4+1
    chosen_pid=sql(f"SELECT pid FROM {VIEW} WHERE slot={chosen_slot} AND admission_state='admitted'")
    victim=next(connection for connection in active if connection.query('SELECT pg_backend_pid()')[0]==chosen_pid)
    victim.query("\\set VERBOSITY verbose\nSET LOCAL deadlock_timeout='100ms'")
    pool=ThreadPoolExecutor(max_workers=2)
    try:
        bulk_wait=pool.submit(bulk.query,STREAM);wait_count('waiting_for_stream',1)
        cycle=pool.submit(victim.query,'SELECT lake_replication.admit_bulk()')
        try:cycle.result(timeout=15)
        except RuntimeError as error:
            victim.error_reader.join(timeout=2)
            observed['deadlock_error']=str(error)+''.join(victim.errors)
            q.check('mixed bulk/stream cycle aborts with SQLSTATE 40P01','40P01' in observed['deadlock_error'],True)
        else:raise AssertionError('mixed admission cycle unexpectedly completed')
        bulk_wait.result(timeout=15)
        bulk.query('ROLLBACK')
        for connection in active:
            if connection is not victim:connection.query('ROLLBACK')
        q.check('mixed cycle releases every streaming slot',count(),0)
        q.check('mixed cycle releases bulk permit',sql('SELECT count(*) FROM lake_replication.bulk_admission'),'0')
        retry=client('pool_other');retry.query('BEGIN; '+STREAM+'; SELECT lake_replication.admit_bulk(); ROLLBACK')
        q.check('mixed transaction can retry after the cycle',count(),0)
    finally:
        for connection in [bulk]+active:connection.close()
        pool.shutdown(wait=True,cancel_futures=True)
