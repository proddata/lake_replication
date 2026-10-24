"""Validate weighted exact resampling against an independent exhaustive oracle."""
import importlib.util
import itertools
import math
from pathlib import Path
import statistics
import unittest

spec=importlib.util.spec_from_file_location('pair_extension',Path(__file__).with_name('pair_extension.py'))
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)


class ExactResamplingTests(unittest.TestCase):
    def test_matches_ordered_draw_enumeration(self):
        for values in [[1],[.9,1.1],[.8,1,1.2],[.9,1,1,1.1],[.95,1,1.02,1.04,1.11],[.8,.9,1,1.02,1.1,1.2]]:
            with self.subTest(values=values):
                medians=sorted(statistics.median(sample) for sample in itertools.product(values,repeat=len(values)))
                expected=[medians[math.ceil(len(medians)*f)-1] for f in [.025,.975]]
                result=p.exact_bootstrap(values)
                self.assertEqual(result['bootstrap_median_95_interval'],expected)
                self.assertEqual(result['bootstrap_samples'],len(medians))

    def test_ten_pairs_keep_all_resampling_weight(self):
        result=p.exact_bootstrap([1]*10)
        self.assertEqual(result['bootstrap_samples'],10**10)
        self.assertEqual(result['enumerated_count_vectors'],92378)
        self.assertEqual(result['bootstrap_median_95_interval'],[1,1])

    def test_invalid_ratios_are_rejected(self):
        for values in [[],[0],[float('nan')],[float('inf')],[1]*11]:
            with self.assertRaises(ValueError):p.exact_bootstrap(values)
