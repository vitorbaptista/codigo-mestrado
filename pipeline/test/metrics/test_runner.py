# -*- coding: utf-8 -*-


import unittest

from pipeline.metrics.runner import Runner


class TestRunner(unittest.TestCase):
    def test_run(self):
        votes = [
            {'vote1': 0, 'vote2': 1, 'vote3': 0},
            {'vote1': 1, 'vote2': 0, 'vote3': 1},
        ]
        result = {
            'vote1': votes[0]['vote1'],
            'vote2': votes[0]['vote2'],
            'vote3': votes[0]['vote3'],
        }
        mock_calculate = lambda votes: votes[0]

        self.assertEqual(Runner.run(votes, mock_calculate), result)
