# -*- coding: utf-8 -*-


class RiceIndex(object):
    YES = 1
    NO = 0

    @classmethod
    def calculate(cls, votes):
        """Calculates the Rice Index ignoring null votes."""
        num_yes = votes.count(cls.YES)
        num_no = votes.count(cls.NO)
        total = num_yes + num_no

        return abs(num_yes - num_no) / total

    @classmethod
    def calculate_adjusted(cls, votes):
        """Calculates the Adjusted Rice Index ignoring null votes."""
        rice_index = cls.calculate(votes)
        total = votes.count(cls.YES) + votes.count(cls.NO)

        return (total * (rice_index ** 2) + total - 2) / (2 * (total - 1))
