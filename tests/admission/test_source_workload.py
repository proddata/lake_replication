"""Independent source sessions must isolate slow streams and stop on failures."""
import importlib.util
from pathlib import Path
import threading
import time
import unittest

spec=importlib.util.spec_from_file_location('source_workload',Path(__file__).resolve().parents[2]/'experiments/admission/source_workload.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class Connection:
    def __init__(self,xid,gate=None,finished=None):
        self.xid,self.gate,self.finished=xid,gate,finished

    def query(self,statement):
        if statement.startswith('INSERT'):
            if self.gate and not self.gate.wait(3):raise TimeoutError('test gate was not released')
            return []
        if self.finished:self.finished.set()
        return [f'{self.xid}|1000.25']


class SourceWorkloadTests(unittest.TestCase):
    def test_blocked_stream_does_not_delay_other_streams(self):
        gate=threading.Event();finished=[threading.Event(),threading.Event()]
        clients=[Connection(1,gate),Connection(2,finished=finished[0]),Connection(3,finished=finished[1])]
        commits=[[],[],[]];errors=[];stop=threading.Event()
        producers=m.start(clients,1,time.monotonic(),stop,commits,errors)
        try:
            self.assertTrue(all(event.wait(2) for event in finished))
            self.assertEqual(commits[0],[])
        finally:
            gate.set();m.join(producers,3)
        self.assertEqual(errors,[])
        self.assertEqual([len(c) for c in commits],[1,1,1])
        self.assertEqual([c[0]['xid'] for c in commits],[1,2,3])
        self.assertEqual([c[0]['transaction'] for c in commits],[1,1,1])
        for rows in commits:
            self.assertLessEqual(rows[0]['insert_started_seconds'],rows[0]['insert_completed_seconds'])
            self.assertLessEqual(rows[0]['insert_completed_seconds'],rows[0]['commit_observed_seconds'])

    def test_source_error_signals_shared_cancellation(self):
        class Failed:
            def query(self,statement):raise RuntimeError('source unavailable')
        stop=threading.Event();commits=[[],[],[]];errors=[]
        producers=m.start([Failed(),Connection(2),Connection(3)],1,time.monotonic(),stop,commits,errors)
        m.join(producers,3)
        self.assertTrue(stop.is_set())
        self.assertEqual(errors,['source unavailable'])
        self.assertEqual(commits[0],[])

    def test_join_reports_a_blocked_producer(self):
        gate=threading.Event();stop=threading.Event();commits=[[]];errors=[]
        producers=m.start([Connection(1,gate)],1,time.monotonic(),stop,commits,errors)
        try:
            with self.assertRaises(TimeoutError):m.join(producers,.01)
        finally:
            gate.set();m.join(producers,3)
        self.assertEqual(errors,[])


if __name__=='__main__':unittest.main()
