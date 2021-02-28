# -*- coding: utf-8 -*-

import pytest

from datetime import date
from nfetl._datetime import _date


@pytest.mark.parameterize("dtInput, intOutput",
                          [(_date(date.today().year, 9, 8),
                            date.today().year),
                           (_date(date.today().year, 2, 1),
                            date.today().year - 1)])
def test_nfl_year(dtInput, intOutput):
    assert dtInput.nfl_year() == intOutput
