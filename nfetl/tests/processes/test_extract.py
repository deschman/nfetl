# -*- coding: utf-8 -*-
"""Test extraction functions in nfetl.processes.extract"""


# %% Imports
# %%% Py3 Standard
from typing import List

# %%% 3rd Party
import pytest
import pandas as pd

# %%% User Defined
from nfetl.processes.extract import get_url_data, get_update
from nfetl.core import _config
from nfetl._datetime import _date


# %% Functions
def test_get_url_data() -> None:
    actual_data: pd.DataFrame = get_url_data(
        _config['test']['url'], _config['test']['extract_columns'].split(', '))
    test_data: pd.DataFrame = pd.read_hdf(_config['DEFAULT']['test_url_data'],
                                          'test_url_data')
    assert (actual_data.to_numpy() == test_data.to_numpy()).all()


def test_get_update() -> None:
    dfs: List[pd.DataFrame] = get_update(_date.today().nfl_year)
    try:
        not_empty: List[bool] = [len(df.index) > 0 for df in dfs.values()]
    except AttributeError:
        not_empty: List[bool] = [False]
    urls: List[str] = [key for key in _config.sections()]
    assert all(not_empty) and len(dfs) == len(urls) - 1
