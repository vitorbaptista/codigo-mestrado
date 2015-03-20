# -*- coding: utf-8 -*-


import unittest

from pipeline.metrics.runner import Runner


YES = 1
NO = 0


class RunnerTest(unittest.TestCase):
    def test_run(self):
        votes = [
            {'vote1': NO, 'vote2': YES, 'vote3': NO},
            {'vote1': YES, 'vote2': NO, 'vote3': YES},
        ]
        result = {
            'vote1': votes[0]['vote1'],
            'vote2': votes[0]['vote2'],
            'vote3': votes[0]['vote3'],
        }
        mock_calculate = lambda votes: votes[0]

        self.assertEqual(Runner.run(votes, mock_calculate), result)
