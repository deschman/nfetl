# -*- coding: utf-8 -*-
"""Load data into database landing and staging schemas."""


# %% Imports
# %%% Py3 Standard
from datetime import datetime
from typing import Dict

# %%% 3rd Party
import pandas as pd

# %%% User Defined
from nfetl.db import DB


# %% Variables
__all__ = ['land_data', 'land_all_data']


# %% Functions
# %%% Private
def _timestamp_data(df: pd.DataFrame) -> pd.DataFrame:
    results: pd.DataFrame = df.copy()
    results.insert(
        0, 'LOAD_STAMP', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), True)
    return results

# %%% Public
def land_data(db: DB, dataset_name: str, df: pd.DataFrame) -> None:
    """
    Load named DataFrame into LANDING schema of DB.

    Parameters
    ----------
    name : str
        Name of DataFrame.
    df : pd.DataFrame
        Data in pandas DataFrame.
    """
    _timestamp_data(df).to_sql(
        'LND_' + dataset_name, db.connection, if_exists='replace')

def land_all_data(db: DB, dfs: Dict[str, pd.DataFrame]) -> None:
    """
    Land all DataFrams in dictionary of named DataFrames.

    Parameters
    ----------
    dfs : Dict[str, pd.DataFrame]
        Dicitonary of named DataFrames.
    """
    [land_data(db, i[0], i[1]) for i in dfs.items()]
