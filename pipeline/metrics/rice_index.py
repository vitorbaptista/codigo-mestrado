# -*- coding: utf-8 -*-


class RiceIndex(object):
    def __init__(self, yes_value=1, no_value=0):
        self.yes = yes_value
        self.no = no_value

    def calculate(self, votes):
        """Calculates the Rice Index ignoring null votes."""
        num_yes = votes.count(self.yes)
        num_no = votes.count(self.no)
        total = num_yes + num_no
        if total == 0:
            return

        return abs(num_yes - num_no) / total

    def calculate_adjusted(self, votes):
        """Calculates the Adjusted Rice Index ignoring null votes."""
        rice_index = self.calculate(votes)
        total = votes.count(self.yes) + votes.count(self.no)
        if total == 0:
            return

        return (total * (rice_index ** 2) + total - 2) / (2 * (total - 1))
