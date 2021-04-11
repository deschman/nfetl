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
from nfetl._config import _config as config
from nfetl._datetime import date


# %% Functions
def test_get_url_data():
    actual_data: pd.DataFrame = get_url_data(
        config['URLs']['test'], config['Columns_extract']['test'].split(', '))
    test_data: pd.DataFrame = pd.read_hdf(config['Data']['test_url_data'],
                                          'test_url_data')
    assert (actual_data.to_numpy() == test_data.to_numpy()).all()

def test_get_update():
    dfs: List[pd.DataFrame] = get_update(date.today().nfl_year)
    try:
        not_empty: List[bool] = [len(df.index) > 0 for df in dfs.values()]
    except AttributeError:
        not_empty: List[bool] = [False]
    urls: List[str] = [key for key in config['URLs'].keys()]
    assert all(not_empty) and len(dfs) == len(urls) - 1
