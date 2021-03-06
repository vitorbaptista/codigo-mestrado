# -*- coding: utf-8 -*-

import unittest
import os
import io
import csv
import collections
import numpy as np

from pipeline.metrics.runner import Runner
from pipeline.metrics.runner import Rollcall


class TestRunner(unittest.TestCase):
    def test_run_writes_result_in_output(self):
        csv_path = self._get_csv_path('example_votes.csv')
        args = ['--input', csv_path]
        output = io.StringIO()
        expected_result = [
            ['poll1', 'poll2', 'poll3', 'poll4'],
            ['1', '1', '1', '1'],
            ['1', '1', '', ''],
            ['1', '1', '', ''],
            ['1', '0', '0', ''],
            ['1', '0', '', ''],
            ['1', '0', '', '']
        ]

        Runner().run(args, output)
        output.seek(0)
        result = [row for row in csv.reader(output)]

        self.assertEqual(result, expected_result)

    def test_main(self):
        csv_path = self._get_csv_path('example_votes.csv')
        expected_result = [collections.OrderedDict([
            ('poll1', 1),
            ('poll2', 0.4),
            ('poll3', 0),
            ('poll4', None),
        ])]

        res = Runner().main(csv_path)
        self.assertEqual(res, expected_result)

    def test_main_doesnt_calculate_metric_if_called_without_method(self):
        csv_path = self._get_csv_path('example_votes_with_metadata.csv')
        expected_result = [
            collections.OrderedDict([('poll1', 1), ('poll2', 1),
                                     ('poll3', 1), ('poll4', 1)]),
            collections.OrderedDict([('poll1', 1), ('poll2', 1),
                                     ('poll3', None), ('poll4', None)]),
            collections.OrderedDict([('poll1', 1), ('poll2', 0),
                                     ('poll3', None), ('poll4', None)]),
            collections.OrderedDict([('poll1', 1), ('poll2', 0),
                                     ('poll3', 0), ('poll4', None)]),
            collections.OrderedDict([('poll1', 1), ('poll2', 0),
                                     ('poll3', None), ('poll4', None)]),
            collections.OrderedDict([('poll1', 1), ('poll2', 0),
                                     ('poll3', None), ('poll4', None)]),
        ]

        res = Runner().main(csv_path, metric_method=None)
        self.assertEqual(res, expected_result)

    def test_main_groups_correctly_when_not_calculating_metric(self):
        csv_path = self._get_csv_path('example_votes_with_metadata.csv')
        expected_result = [
            collections.OrderedDict([('party', 'PSOL'),
                                     ('poll1', 1), ('poll2', 0),
                                     ('poll3', None), ('poll4', None)]),
            collections.OrderedDict([('party', 'PT'),
                                     ('poll1', 1), ('poll2', 1),
                                     ('poll3', 1), ('poll4', 1)]),
            collections.OrderedDict([('party', 'PV'),
                                     ('poll1', 1), ('poll2', 0),
                                     ('poll3', 0), ('poll4', None)]),
        ]

        res = Runner().main(csv_path, metric_method=None,
                            groupby='party')
        self.assertEqual(res, expected_result)

    def test_main_groups_doesnt_convert_int_groups_to_float(self):
        csv_path = self._get_csv_path('example_votes_with_metadata.csv')

        res = Runner().main(csv_path, metric_method=None,
                            groupby='id', name='Joao')
        self.assertEqual(type(res[0]['id']), np.int64)

    def test_remove_unanimous_votes_runs_before_filters(self):
        csv_path = self._get_csv_path('example_votes_with_metadata.csv')
        expected_result = [collections.OrderedDict([
            ('poll2', 0.33333333333333337),
            ('poll3', None),
        ])]

        res = Runner().main(csv_path, majority_percentual=0.9, party=['PT'])
        self.assertEqual(res, expected_result)

    def test_main_filtering_by_names(self):
        csv_path = self._get_csv_path('example_votes_with_metadata.csv')
        expected_result = [collections.OrderedDict([
            ('poll1', 1),
            ('poll2', 1),
            ('poll3', None),
            ('poll4', None),
        ])]

        res = Runner().main(csv_path, name=['Joao', 'Pedro'])
        self.assertEqual(res, expected_result)

    def test_main_filtering_by_parties(self):
        csv_path = self._get_csv_path('example_votes_with_metadata.csv')
        expected_result = [collections.OrderedDict([
            ('poll1', 1),
            ('poll2', 0.4),
            ('poll3', None),
            ('poll4', None),
        ])]

        res = Runner().main(csv_path, party=['PT', 'PSOL'])
        self.assertEqual(res, expected_result)

    def test_main_filtering_by_state(self):
        csv_path = self._get_csv_path('example_votes_with_metadata.csv')
        expected_result = [collections.OrderedDict([
            ('poll1', 1),
            ('poll2', 0.33333333333333337),
            ('poll3', None),
            ('poll4', None),
        ])]

        res = Runner().main(csv_path, state=['PB', 'PE'])
        self.assertEqual(res, expected_result)

    def test_main_filtering_ignores_empty_filters(self):
        csv_path = self._get_csv_path('example_votes_with_metadata.csv')
        expected_result = [collections.OrderedDict([
            ('poll1', 1),
            ('poll2', 0.4666666666666667),
            ('poll3', 0),
            ('poll4', None),
        ])]

        res = Runner().main(csv_path, name=[], party=[], state=[])
        self.assertEqual(res, expected_result)

    def test_main_filtering_by_multiple_criteria(self):
        csv_path = self._get_csv_path('example_votes_with_metadata.csv')
        expected_result = [collections.OrderedDict([
            ('poll1', 1),
            ('poll2', 0),
            ('poll3', None),
            ('poll4', None),
        ])]

        res = Runner().main(csv_path,
                            name=['Joao', 'Natalia'],
                            party=['PT', 'PSOL'],
                            state=['PB', 'RJ'])
        self.assertEqual(res, expected_result)

    def test_main_grouping_votes(self):
        csv_path = self._get_csv_path('example_votes_with_metadata.csv')
        expected_result = [collections.OrderedDict([
            ('poll1', 1),
            ('poll2', 0.33333333333333337),
            ('poll3', 0),
            ('poll4', None),
        ])]

        res = Runner().main(csv_path, groupby='party')
        self.assertEqual(res, expected_result)

    def test_main_grouping_votes_ignores_invalid_groupby_column(self):
        csv_path = self._get_csv_path('example_votes_with_metadata.csv')
        Runner().main(csv_path, groupby='invalid_groupby_column')

    def test_main_removes_metadata_columns_from_result(self):
        csv_path = self._get_csv_path('example_votes_with_metadata.csv')
        metadata_columns = Rollcall.METADATA_COLUMNS
        res = Runner().main(csv_path)

        for metadata_column in metadata_columns:
            with self.subTest(metadata_column=metadata_column):
                self.assertNotIn(metadata_column, res)

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

        result = Runner().calculate_metric(votes, mock_calculate)
        self.assertEqual(result, expected_result)

    def test_calculate_metric_with_empty_polls(self):
        votes = []
        expected_result = []
        mock_calculate = lambda votes: votes[0]

        result = Runner().calculate_metric(votes, mock_calculate)
        self.assertEqual(result, expected_result)

    def test_calculate_metric_with_empty_votes(self):
        votes = [[]]
        expected_result = []
        mock_calculate = lambda votes: votes[0]

        result = Runner().calculate_metric(votes, mock_calculate)
        self.assertEqual(result, expected_result)

    def _get_csv_path(self, filename):
        base_path = os.path.dirname(os.path.realpath(__file__))
        data_path = os.path.join(base_path, 'data')
        return os.path.join(data_path, filename)
