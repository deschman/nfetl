# -*- coding: utf-8 -*-
"""Transform data sourced from pro-football-reference.com."""


# %% Imports
# %%% Py3 Standard
from typing import Dict

# %%% 3rd Party
import pandas as pd
# TODO: consider this
# import dask.dataframe as dd


# %% Classes
class TransformationError(Exception):
    """Raised when data transformation encounters a problem."""

    def __init__(self, message) -> None:
        super().__init__(message)


# %% Functions
def clean(df: pd.DataFrame) -> pd.DataFrame:
    pass

def clean_all(dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    pass


# %% Script
if __name__ == '__main__':
    pass
