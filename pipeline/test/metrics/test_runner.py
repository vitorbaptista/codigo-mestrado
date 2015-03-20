# -*- coding: utf-8 -*-

import unittest
import os

from pipeline.metrics.runner import Runner


class TestRunner(unittest.TestCase):
    def test_main(self):
        base_path = os.path.dirname(os.path.realpath(__file__))
        csv_path = os.path.join(base_path, 'data', 'example_votes.csv')
        expected_result = {
            'poll1': 0.4,
            'poll2': 0.0,
            'poll3': None,
            'poll4': None,
        }

        self.assertEqual(Runner.main(csv_path), expected_result)

    def test_calculate_metric(self):
        votes = [
            {'vote1': 0, 'vote2': 1, 'vote3': 0},
            {'vote1': 1, 'vote2': 0, 'vote3': 1},
        ]
        expected_result = {
            'vote1': votes[0]['vote1'],
            'vote2': votes[0]['vote2'],
            'vote3': votes[0]['vote3'],
        }
        mock_calculate = lambda votes: votes[0]

        result = Runner.calculate_metric(votes, mock_calculate)
        self.assertEqual(result, expected_result)
