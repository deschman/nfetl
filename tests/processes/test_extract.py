# -*- coding: utf-8 -*-


# %% Imports
# %%% Py3 Standard
from datetime import date
from typing import List

# %%% 3rd Party
import pytest
import numpy as np
import pandas as pd

# %%% User Defined
from nfetl.processes.extract import get_url_data, get_update
from nfetl._config import _config as config


# %% Functions
def test_get_url_data():
    actual_data: pd.DataFrame = get_url_data(config['URLs']['test'])
    test_data: pd.DataFrame = pd.read_hdf(config['Data']['test_url_data'],
                                          'test_url_data')
    equal: np.array = actual_data.to_numpy() == test_data.to_numpy()
    assert equal.all()

def test_get_update():
    dfs: List[pd.DataFrame] = get_update(date.today().year)
    try:
        not_empty: List[bool] = [len(df.index) > 0 for df in dfs]
    except AttributeError:
        not_empty: List[bool] = [False]
    urls: List[str] = [key for key in config['URLs'].keys()]
    assert all(not_empty) and len(dfs) == len(urls)


# %% Script
if __name__ == '__main__':
    test_get_url_data()
