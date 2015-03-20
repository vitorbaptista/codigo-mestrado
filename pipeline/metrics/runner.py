# -*- coding: utf-8 -*-


import csv
import itertools

from pipeline.metrics.rice_index import RiceIndex


class Runner(object):
    @classmethod
    def main(cls, csv_path):
        """Calculates the adjusted Rice Index polls contained in a CSV

        Args:
            csv_path (string): Path to a CSV file with polls as columns and
                people/groups' votes as rows. The votes should be either 1 for
                YES, 2 for NO, or empty for Not Voted. Example:
                    poll1,poll2,poll3
                    0,,1
                    1,1,1

        Returns:
            dict: A dict with each poll name in the keys and the resulting
                adjusted Rice Index in the values.
        """
        with open(csv_path) as csv_file:
            metric_method = RiceIndex('1', '0').calculate_adjusted
            votes = [v for v in csv.DictReader(csv_file)]

        return cls.run(votes, metric_method)

    @classmethod
    def run(cls, votes, metric_method):
        """Takes list of poll votes and returns result of metric on each poll.

        Args:
            votes (list of dicts): List of vote dicts. Each element represent
                a person (or group) votes on a number of polls. The structure
                looks like:
                    [{'poll1': 0, 'poll2': 1}, {'poll1': 1, 'poll2': 1}]
                This would usually be the result of loading a CSV file.
            metric_method (method): Method that receives a list of votes and
                return a single value.

        Returns:
            dict: Dictionary mapping each poll to the result of executing
                metric_method on its votes. For example:
                    {'poll1': 0, 'poll2': 1}
        """
        vote_names = [v.keys() for v in votes]
        unique_vote_names = set(itertools.chain.from_iterable(vote_names))
        result = {}

        for vote_name in unique_vote_names:
            the_votes = [v[vote_name] for v in votes]
            result[vote_name] = metric_method(the_votes)

        return result
