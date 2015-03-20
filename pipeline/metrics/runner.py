# -*- coding: utf-8 -*-

import csv
import argparse
import sys
import collections

import numpy as np
import pandas as pd

from pipeline.metrics.rice_index import RiceIndex


class Runner(object):
    def __init__(self):
        self.parser = self.__create_parser()

    def run(self, args=sys.argv[1:], output=sys.stdout):
        options = self.parser.parse_args(args)
        result = self.__class__.main(options.csv_path[0],
                                     name=options.name,
                                     party=options.party,
                                     state=options.state)

        writer = csv.DictWriter(output, fieldnames=result.keys())
        writer.writeheader()
        writer.writerow(result)

    @classmethod
    def main(cls, csv_path, **filters):
        """Calculates the adjusted Rice Index polls contained in a CSV

        Args:
            csv_path (string): Path to a CSV file with polls as columns and
                people/groups' votes as rows. The votes should be either 1 for
                YES, 2 for NO, or empty for Not Voted. Example:
                    poll1,poll2,poll3
                    0,,1
                    1,1,1

        Returns:
            OrderedDict: A dict with each poll name in the keys and the
                resulting adjusted Rice Index in the values.
        """
        votes = pd.DataFrame.from_csv(csv_path, index_col=None)
        metric_method = RiceIndex().calculate_adjusted
        metadata = []
        try:
            metadata_columns = ['name', 'party', 'state']
            metadata = votes[metadata_columns]
            votes = votes.drop(metadata_columns, axis=1)
        except (ValueError, KeyError):
            # Ignore if votes don't have the metadata_columns
            pass

        if (len(metadata)):
            for key, values in filters.items():
                if (not values):
                    continue
                criterion = metadata[key].map(lambda k: k in values)
                votes = votes[criterion]

        metrics = cls.calculate_metric(votes, metric_method)
        return collections.OrderedDict(zip(votes.columns, metrics))

    @classmethod
    def calculate_metric(cls, votes, metric_method):
        """Takes list of poll votes and returns result of metric on each poll.

        Args:
            votes (list of lists): List of vote lists. Each element represent
                a person (or group) votes on a number of polls. The structure
                looks like:
                    [[0, 1], [1, 1]]
                This would usually be the result of loading a CSV file.
            metric_method (method): Method that receives a list of votes and
                return a single value.

        Returns:
            list: Result of executing metric_method on each poll. The order of
                the list is the same as the votes received. For example:
                    [0, 1]
        """
        result = []

        if (len(votes) != 0):
            votes = np.array(votes)
            for column_index in range(0, len(votes[0])):
                the_votes = votes[:, column_index]
                result.append(metric_method(the_votes))

        return result

    def __create_parser(self):
        parser = argparse.ArgumentParser(
            description="Calculates adjusted rice index on a CSV with votes"
        )
        parser.add_argument(
            "csv_path", nargs=1, type=str,
            help="path for CSV with polls as columns and rows with votes"
        )
        parser.add_argument(
            "--name", nargs="*", type=str, default=[],
            help="deputies to use when calculating cohesion (default: all)"
        )
        parser.add_argument(
            "--party", nargs="*", type=str, default=[],
            help="parties to use when calculating cohesion (default: all)"
        )
        parser.add_argument(
            "--state", nargs="*", type=str, default=[],
            help="states to use when calculating cohesion (default: all)"
        )
        return parser
