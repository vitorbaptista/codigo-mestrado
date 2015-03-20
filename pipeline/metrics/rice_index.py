# -*- coding: utf-8 -*-


class RiceIndex(object):
    YES = 1
    NO = 0

    @classmethod
    def calculate(cls, votes):
        num_yes = votes.count(cls.YES)
        num_no = votes.count(cls.NO)
        total = len(votes)

        return abs(num_yes - num_no) / total
