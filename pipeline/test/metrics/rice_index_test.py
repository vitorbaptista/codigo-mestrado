# -*- coding: utf-8 -*-


import unittest

from pipeline.metrics.rice_index import RiceIndex


YES = 1
NO = 0


class TestRiceIndex(unittest.TestCase):
    def test_yes_and_no_values(self):
        self.assertEqual(RiceIndex.YES, YES)
        self.assertEqual(RiceIndex.NO, NO)

    def test_calculate_rice_index(self):
        test_cases = [
            {'test': [NO, YES], 'result': 0},
            {'test': [NO, NO, NO], 'result': 1},
            {'test': [NO, NO, YES], 'result': 0.3333333333333333},
        ]
        for test_case in test_cases:
            test = test_case['test']
            result = test_case['result']

            with self.subTest(test=test, result=result):
                self.assertEqual(RiceIndex.calculate(test), result)
