# -*- coding: utf-8 -*-

import unittest

from pipeline.parties_and_coalitions_changes import PartiesAndCoalitionsChanges


class TestPartiesAndCoalitionsChanges(unittest.TestCase):
    def test_remove_consecutive_duplicates(self):
        data = [
            {'id': 1, 'value': True},
            {'id': 2, 'value': True},
            {'id': 3, 'value': True},
            {'id': 4, 'value': False},
            {'id': 5, 'value': True},
            {'id': 6, 'value': True},
        ]
        expected_result = [
            {'id': 1, 'value': True},
            {'id': 4, 'value': False},
            {'id': 5, 'value': True},
        ]
        obj = PartiesAndCoalitionsChanges()
        self.assertEqual(obj._remove_consecutive_duplicates(data, 'value'),
                         expected_result)
