# -*- coding: utf-8 -*-

from datetime import *


class _date(date):

    __doc__ = date.__doc__

    def nfl_year(self):
        """year (1-9999)"""
        if self.month in [10, 11, 12] or (self.month == 9 and
                                          self.day / 7 > 1):
            return self.year
        else:
            return self.year - 1
