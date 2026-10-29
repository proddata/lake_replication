"""Keep diagnostic phase timings consistent with the existing end-to-end lag."""
import importlib.util
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('admission_measure',ROOT/'experiments/admission/admission_measure.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class PublicationPhaseTests(unittest.TestCase):
    def test_components_sum_to_lag_without_changing_source_observation(self):
        source=dict(xid=123,commit_epoch=100.0,transaction=1)
        result=m.publication_phases(source,100.5,100.75,101.0)
        self.assertEqual(result['source_to_journal_insert_ms'],500)
        self.assertEqual(result['journal_insert_to_batch_ms'],250)
        self.assertEqual(result['batch_to_publication_commit_ms'],250)
        self.assertEqual(result['lag_ms'],1000)
        self.assertEqual(sum(result[k] for k in ['source_to_journal_insert_ms',
            'journal_insert_to_batch_ms','batch_to_publication_commit_ms']),result['lag_ms'])
        self.assertEqual(source,dict(xid=123,commit_epoch=100.0,transaction=1))

    def test_coincident_markers_are_valid(self):
        result=m.publication_phases(dict(commit_epoch=100.0),100.0,100.0,100.0)
        self.assertEqual(result['lag_ms'],0)

    def test_out_of_order_markers_do_not_create_misleading_durations(self):
        for values in [(99,100,101),(100.5,100.25,101),(100.5,101,100.75)]:
            with self.subTest(values=values),self.assertRaises(AssertionError):
                m.publication_phases(dict(commit_epoch=100.0),*values)


if __name__=='__main__':unittest.main()
