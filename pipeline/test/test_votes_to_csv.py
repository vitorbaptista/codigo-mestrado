# -*- coding: utf-8 -*-

import unittest

from pipeline.votes_to_csv import VotesToCSV


class TestVotesToCSV(unittest.TestCase):
    def test_legislature_dates(self):
        expected_dates = {
            47: None,
            48: ('1987-02-01', '1991-01-31'),
            52: ('2003-02-01', '2007-01-31'),
            55: ('2015-02-01', '2019-01-31'),
        }

        for legislature, dates in expected_dates.items():
            with self.subTest(legislature=legislature):
                self.assertEqual(VotesToCSV()._legislature_dates(legislature),
                                 dates)
