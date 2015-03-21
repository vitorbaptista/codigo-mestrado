# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd


class Rollcall(object):
    METADATA_COLUMNS = ["name", "party", "state"]

    def __init__(self, data):
        metadata = []
        try:
            metadata = data[self.METADATA_COLUMNS]
            data = data.drop(self.METADATA_COLUMNS, axis=1)
        except (ValueError, KeyError):
            # Ignore if votes don't have the metadata_columns
            pass
        self.data = data
        self.metadata = metadata

    @classmethod
    def from_csv(cls, csv_path):
        votes = pd.DataFrame.from_csv(csv_path, index_col=None)
        return cls(votes)

    def filter(self, filters):
        if len(self.metadata):
            self.__apply_filters(filters)
        return self

    def median_votes_groupped_by(self, groupby):
        if groupby in self.metadata:
            return self.__get_groups_median_votes(groupby)
        return self.data

    def remove_unanimous_votes(self, majority_percentual):
        """Remove votes with majority larget than `majority_percentual`

        It ignores NULL votes.
        """
        def unanimous_columns(column):
            counts = np.unique(column[pd.notnull(column)],
                               return_counts=True)[1]
            total = sum(counts)
            for count in counts:
                percentual = count/total
                if percentual >= majority_percentual:
                    return False
            return len(counts) > 0
        if majority_percentual is not None:
            filters = self.data.apply(unanimous_columns)
            self.data = self.data[filters.index[filters]]
        return self

    def __apply_filters(self, filters):
        criteria = []
        for key, values in filters.items():
            if not values:
                continue
            criterion = self.metadata[key].map(lambda k: k in values)
            if len(criteria) == 0:
                criteria = criterion
            criteria = criteria & criterion
        if len(criteria):
            self.metadata = self.metadata[criteria]
            self.data = self.data[criteria]
        return self

    def __get_groups_median_votes(self, groupby):
        def mode_removing_nulls(arr):
            res = mode(arr[pd.notnull(arr)])[0]
            if len(res):
                return res[0]
        return self.data.groupby(self.metadata[groupby])\
                        .aggregate(mode_removing_nulls)


# Copied from scipy.stats to avoid importing an entire library for a
# single method
# http://git.io/h1Pv
def mode(a, axis=0):
    a, axis = _chk_asarray(a, axis)
    if a.size == 0:
        return np.array([]), np.array([])

    scores = np.unique(np.ravel(a))       # get ALL unique values
    testshape = list(a.shape)
    testshape[axis] = 1
    oldmostfreq = np.zeros(testshape, dtype=a.dtype)
    oldcounts = np.zeros(testshape, dtype=int)
    for score in scores:
        template = (a == score)
        counts = np.expand_dims(np.sum(template, axis), axis)
        mostfrequent = np.where(counts > oldcounts, score, oldmostfreq)
        oldcounts = np.maximum(counts, oldcounts)
        oldmostfreq = mostfrequent
    return mostfrequent, oldcounts


def _chk_asarray(a, axis):
    if axis is None:
        a = np.ravel(a)
        outaxis = 0
    else:
        a = np.asarray(a)
        outaxis = axis
    return a, outaxis
