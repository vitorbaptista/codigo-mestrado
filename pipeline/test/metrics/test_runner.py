# -*- coding: utf-8 -*-

import unittest
import os
import io
import csv
import collections

from pipeline.metrics.runner import Runner


class TestRunner(unittest.TestCase):
    def test_run_writes_result_in_output(self):
        base_path = os.path.dirname(os.path.realpath(__file__))
        csv_path = os.path.join(base_path, 'data', 'example_votes.csv')
        args = [csv_path]
        output = io.StringIO()
        expected_result = [
            ['poll1', 'poll2', 'poll3', 'poll4'],
            ['0.4', '0.0', '', ''],
        ]

        Runner().run(args, output)
        output.seek(0)
        result = [row for row in csv.reader(output)]

        self.assertEqual(result, expected_result)

    def test_main(self):
        base_path = os.path.dirname(os.path.realpath(__file__))
        csv_path = os.path.join(base_path, 'data', 'example_votes.csv')
        expected_result = collections.OrderedDict([
            ('poll1', 0.4),
            ('poll2', 0.0),
            ('poll3', None),
            ('poll4', None),
        ])

        res = Runner.main(csv_path)
        self.assertEqual(res, expected_result)

    def test_calculate_metric(self):
        votes = [
            [0, 1, 0],
            [1, 0, 1],
        ]
        expected_result = [
            votes[0][0],
            votes[0][1],
            votes[0][2],
        ]
        mock_calculate = lambda votes: votes[0]

        result = Runner.calculate_metric(votes, mock_calculate)
        self.assertEqual(result, expected_result)
