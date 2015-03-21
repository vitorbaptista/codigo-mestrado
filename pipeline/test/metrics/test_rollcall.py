# -*- coding: utf-8 -*-

import unittest

import pandas as pd

from pipeline.metrics.rollcall import Rollcall


class TestRollcall(unittest.TestCase):
    def test_filters(self):
        data = pd.core.frame.DataFrame([
            {'name': 'Joao', 'party': 'PT', 'state': 'PB', 'poll1': 1},
            {'name': 'Pedro', 'party': 'PSOL', 'state': 'PE', 'poll1': 0},
        ])
        filters = {
            'state': ['PE'],
        }
        expected_votes = {'poll1': {1: 0}}
        expected_metadata = {
            'name': {1: 'Pedro'},
            'party': {1: 'PSOL'},
            'state': {1: 'PE'}
        }

        rollcall = Rollcall(data).filter(filters)

        self.assertEqual(rollcall.data.to_dict(), expected_votes)
        self.assertEqual(rollcall.metadata.to_dict(), expected_metadata)

    def test_median_votes_groupped_by(self):
        data = pd.core.frame.DataFrame([
            {'name': 'Joao', 'party': 'PT', 'state': 'PB', 'poll1': 1},
            {'name': 'Joana', 'party': 'PT', 'state': 'PB', 'poll1': 1},
            {'name': 'Marcio', 'party': 'PT', 'state': 'PB', 'poll1': 0},
            {'name': 'Pedro', 'party': 'PSOL', 'state': 'PE', 'poll1': 0},
        ])
        expected_result = {
            'poll1': {
                'PT': 1,
                'PSOL': 0,
            }
        }
        groupby = 'party'

        rollcall = Rollcall(data)
        result = rollcall.median_votes_groupped_by(groupby)

        self.assertEqual(result.to_dict(), expected_result)

    def test_remove_unanimous_votes(self):
        data = pd.core.frame.DataFrame([
            {'poll1': 1, 'poll2': 1, 'poll3': None},
            {'poll1': 1, 'poll2': 1, 'poll3': None},
            {'poll1': 1, 'poll2': 0, 'poll3': None},
            {'poll1': 0, 'poll2': 0, 'poll3': None},
            {'poll1': None, 'poll2': 0, 'poll3': None},
        ])

        rollcall = Rollcall(data).remove_unanimous_votes(0.75)

        # FIXME: Decide if we'll remove polls with no votes
        self.assertEqual(rollcall.data.columns.tolist(), ['poll2'])

    def test_remove_unanimous_votes_doesnt_remove_if_percentual_is_None(self):
        data = pd.core.frame.DataFrame([
            {'poll1': 1},
            {'poll1': 1},
        ])

        rollcall = Rollcall(data).remove_unanimous_votes(None)

        self.assertEqual(rollcall.data.columns.tolist(), ['poll1'])
