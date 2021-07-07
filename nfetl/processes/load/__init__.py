# -*- coding: utf-8 -*-
"""Load data into database landing and staging schemas."""


# %% Imports
# %%% Py3 Standard
from datetime import datetime
from typing import Dict, Tuple, List

# %%% 3rd Party
import pandas as pd

# %%% User Defined
from nfetl.core import _DB


# %% Variables
__all__ = ['land_data', 'land_all_data']


# %% Functions
# %%% Private
def _timestamp_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add LOAD_STAMP column to df with current timestamp.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to be timestamped.

    Returns
    -------
    results : pd.DataFrame
        Timestamped DataFrame.
    """
    results: pd.DataFrame = df.copy()

    if 'LOAD_STAMP' in results.columns:
        results.loc[:, 'LOAD_STAMP'] = results.loc[:, 'LOAD_STAMP'].fillna(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    else:
        results.insert(
            0, 'LOAD_STAMP', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), True)
    return results


def _select_data_subset(db: _DB,
                        dataset_name: str,
                        df: pd.DataFrame,
                        lookup_columns: List[str]) -> pd.DataFrame:
    """
    Select subset of dataset from db with df excluded by lookup_columns.

    Parameters
    ----------
    db : DB
        Database where dataset is found.
    dataset_name : str
        Name of dataset.
    df : pd.DataFrame
        Data to be excluded from subset.
    lookup_columns : List[str]
        Columns that will be used to look up df data.

    Returns
    -------
    pd.DataFrame
        Subset of dataset.
    """
    concat_index: str = str(
        ['CAST("' + i + '" AS TEXT)' for i in lookup_columns]
        )[1:-1].replace("'", '').replace(', ', " || '_' || ")
    lookup_list: str = str(
        ['_'.join([row.__getattr__(c) for c in lookup_columns])
         for i, row in df.iterrows()])[1:-1]
    # If list is blank, should be empty quotes
    lookup_list = "''" if lookup_list == '' else lookup_list
    return pd.read_sql(
        f"SELECT * FROM {dataset_name} WHERE {concat_index} NOT IN ({lookup_list})",
        db.connection)


# %%% Public
def load_data(db: _DB,
              dataset_name: str,
              df: pd.DataFrame,
              timestamp: bool = True,
              if_exists: str = 'replace') -> None:
    """
    Load named DataFrame into DB with prefix, if applicable.

    Parameters
    ----------
    db : nfetl.DB
        Database where data is landed.
    name : str
        Name of DataFrame.
    df : pd.DataFrame
        Data in pandas DataFrame.
    timestamp : bool, default False
        If true, a LOAD_STAMP column will be added with a timestamp.
    if_exists : {'replace', 'append', 'fail'}, default 'replace'
        How load will behave if the table already exists.
    """
    df = _timestamp_data(df) if timestamp else df
    df.to_sql(dataset_name, db.connection, if_exists=if_exists, index=False)


def load_multiple(db: _DB,
                  dfs: Dict[str, pd.DataFrame],
                  prefix: str = '',
                  timestamp: bool = True,
                  if_exists: str = 'replace') -> None:
    """
    Load all DataFrams in dictionary of named DataFrames.

    Load named DataFrame into DB with prefix, if applicable.

    Parameters
    ----------
    db : nfetl.DB
        Database where data is landed.
    dfs : Dict[str, pd.DataFrame]
        Dictionary of named DataFrames.
    prefix : str
        Prefix added to all DataFrame names
    timestamp : bool, default False
        If True, a LOAD_STAMP column will be added with a timestamp.
    if_exists : {'replace', 'append', 'fail'}, default 'replace'
        How load will behave if the table already exists.
    """
    [load_data(db, prefix + i[0], i[1], timestamp, if_exists)
     for i in dfs.items()]


def update_data(db: _DB,
                dataset_name: str,
                df: pd.DataFrame,
                lookup_columns: List[str],
                timestamp: bool = True) -> None:
    """
    Update dataset with data in df.

    Parameters
    ----------
    db : DB
        Database where dataset exists.
    dataset_name : str
        Name of dataset.
    df : pd.DataFrame
        Data to update dataset.
    lookup_columns : List[str]
        Columns to look up update dataset.
    timestamp : bool, default False
        If True, df will be timestamped.
    """
    data_subset: pd.DataFrame = _select_data_subset(db,
                                                    dataset_name,
                                                    df,
                                                    lookup_columns)
    df = data_subset.append(df, ignore_index=True)

    df = _timestamp_data(df) if timestamp else df

    df.to_sql(dataset_name,
              db.connection,
              if_exists='replace',
              index=False)


def delete_data(db: _DB,
                dataset_name: str,
                df: pd.DataFrame,
                lookup_columns: List[str]) -> None:
    """
    Delete data in df from dataset.

    Parameters
    ----------
    db : DB
        Database where dataset exists.
    dataset_name : str
        Name of dataset.
    df : pd.DataFrame
        Data to delete from dataset.
    lookup_columns : List[str]
        Columns to look up delete dataset.
    """
    _select_data_subset(db, dataset_name, df, lookup_columns).to_sql(
        dataset_name, db.connection, if_exists='replace', index=False)


def process_all_updates(db: _DB,
                        dfs: List[Tuple[str,
                                        pd.DataFrame,
                                        List[str]]]) -> None:
    """
    Update multiple datasets performing inserts, updates, and deletes.

    Parameters
    ----------
    db : DB
        Database where dataset will be updated.
    dfs : List[Tuple[str, pd.DataFrame, List[str]]]
        Contains dataset name, insert DataFrame, update DataFrame, delete
        DataFrame, and lookup_columns.
    """
    [load_data(db, i[0], i[1], if_exists='append') for i in dfs]
    [update_data(db, i[0], i[2], i[4]) for i in dfs]
    [delete_data(db, i[0], i[3], i[4]) for i in dfs]
