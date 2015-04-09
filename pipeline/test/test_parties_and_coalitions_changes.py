# -*- coding: utf-8 -*-

import unittest

from pipeline.parties_and_coalitions_changes import PartiesAndCoalitionsChanges


class TestPartiesAndCoalitionsChanges(unittest.TestCase):
    def test_remove_unique_rows(self):
        data = [{'id': 1}, {'id': 1}, {'id': 2}]
        expected_result = [{'id': 1}, {'id': 1}]
        res = PartiesAndCoalitionsChanges()._remove_unique_rows(data, 'id')
        self.assertEqual(res, expected_result)
