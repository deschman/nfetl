# -*- coding: utf-8 -*-

# %% Imports
# %%% 3rd Party
import pytest

# %%% User Defined
from datetime import date
from nfetl._datetime import _date


# %% Functions
@pytest.mark.parametrize("dtInput, intOutput",
                         [(_date(date.today().year, 9, 8),
                           date.today().year),
                          (_date(date.today().year, 2, 1),
                           date.today().year - 1)])
def test_nfl_year(dtInput, intOutput) -> None:
    assert dtInput.nfl_year == intOutput
