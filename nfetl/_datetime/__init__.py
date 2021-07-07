# -*- coding: utf-8 -*-
"""Adds nfl_year attribute to standard python date object."""


# %% Imports
from datetime import date


# %% Classes
class _date(date):
    __doc__ = date.__doc__

    # %%% Functions
    @property
    def nfl_year(self) -> int:
        """
        Get nfl_year property.

        Returns
        -------
        int
            NFL season year.
        """
        if self.month in [10, 11, 12] or (self.month == 9 and
                                          self.day / 7 > 1):
            return self.year
        else:
            return self.year - 1


date = _date
