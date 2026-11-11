"""Independent durable source producers for the three-stream fairness workload."""
import threading
import time


def start(connections,transactions,started,stop,commits,errors):
    def produce(index,connection):
        try:
            for tx in range(1,transactions+1):
                delay=started+(tx-1)*.5-time.monotonic()
                if delay>0 and stop.wait(delay):return
                if stop.is_set():return
                began=time.monotonic()-started
                connection.query(f"INSERT INTO fair_items_{index} SELECT {tx}::bigint*20+n,{tx},repeat('x',128) FROM generate_series(1,20) n")
                inserted=time.monotonic()-started
                xid,epoch=connection.query(f'SELECT xmin::text,extract(epoch FROM pg_xact_commit_timestamp(xmin)) FROM fair_items_{index} WHERE tx={tx} LIMIT 1')[0].split('|')
                commits[index].append(dict(transaction=tx,xid=int(xid),commit_epoch=float(epoch),
                    insert_started_seconds=began,insert_completed_seconds=inserted,
                    commit_observed_seconds=time.monotonic()-started))
        except Exception as error:
            errors.append(str(error));stop.set()
    producers=[threading.Thread(target=produce,args=(i,connection),name=f'source-stream-{i}')
        for i,connection in enumerate(connections)]
    for producer in producers:producer.start()
    return producers


def join(producers,timeout):
    deadline=time.monotonic()+timeout
    for producer in producers:producer.join(timeout=max(0,deadline-time.monotonic()))
    if any(producer.is_alive() for producer in producers):
        raise TimeoutError('source producer did not stop within the shared deadline')
