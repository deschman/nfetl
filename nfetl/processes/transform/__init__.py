# -*- coding: utf-8 -*-
"""Transform data sourced from pro-football-reference.com."""


# %% Imports
# %%% Py3 Standard
from typing import List, Tuple, Optional, Any, Dict

# %%% 3rd Party
import pandas as pd
from dask import distributed

# %%% User-Defined
from nfetl.core import _config


# %% Variables
# %%% System
__all__ = ['clean_data',
           'clean_all_data',
           'define_archive',
           'define_all_archives'
           'split_update',
           'split_all_updates']

# %%% Private
_client: object = distributed.Client(processes=False)


# %% Classes
class TransformationError(Exception):
    """Raised when data transformation encounters a problem."""

    def __init__(self, message) -> None:
        super().__init__(message)


# %% Functions
def clean_data(source: pd.DataFrame,
               nk_columns: List[str],
               name: str = '') -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Validate source data contains no violations of nk_columns.

    Parameters
    ----------
    source : pd.DataFrame
        Data to be validated.
    nk_columns : List[str]
        Columns to be checked for key violations.
    name : str, optional
        Dataset name. Passed through if provided.

    Returns
    -------
    pd.DataFrame
        Data with key violations removed.
    pd.DataFrame
        Data removed from source due to key violation.
    Optional[str]
        Dataset name if provided
    """
    """if name in _config.sections():
        transform_columns: List[str] = _config[name]['transform_columns'].split(', ')
        source.rename(
            columns={s: transform_columns[source.columns.to_list().index(s)]
                     for s in source.columns},
            inplace=True)"""

    clean_data: pd.DataFrame = source.drop_duplicates(nk_columns)
    removed_data: pd.DataFrame = source.loc[~source.index.isin(
        clean_data.index)].reset_index(drop=True)
    clean_data.reset_index(drop=True, inplace=True)

    if name == '':
        return (clean_data, removed_data)
    else:
        return (clean_data, removed_data, name)


def clean_all_data(sources_nk_tuples: List[Tuple[pd.DataFrame, List[str], str]]
                   ) -> Tuple[Dict[str, pd.DataFrame]]:
    """
    Validate all source data contains no violations of nk_columns.

    Makes multiple calls to clean_data for each name, source, nk_columns tuple
    in sources_nk_tuples list.

    Parameters
    ----------
    sources_nk_tuples : List[Tuple[pd.DataFrame, List[str], str]]
        Tuples of source data, accompanying nk_column list, and data name.

    Returns
    -------
    Tuple[Dict[str, pd.DataFrame]]
        De-duplicated data dictionary and duplicate data dictionary in a tuple.
    """
    results: List[Tuple[str, pd.DataFrame]] = _client.gather(
        [_client.submit(clean_data,
                        source_nk_tuple[0],
                        source_nk_tuple[1],
                        source_nk_tuple[2])
         for source_nk_tuple in sources_nk_tuples])
    return ({r[2]: r[0] for r in results}, {r[2]: r[1] for r in results})


def define_archive(source: pd.DataFrame,
                   target: pd.DataFrame,
                   lookup_columns: List[str],
                   name: str = '') -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Categorize source data by if lookup_column appears in target data.

    Parameters
    ----------
    source : pd.DataFrame
        Data to be categorized.
    target : pd.DataFrame
        Data to be searched.
    lookup_columns : List[str]
        Column(s) to be looked up.
    name : str
        Dataset name. Passed through if provided.

    Returns
    -------
    Tuple
        pd.DataFrame
            Categorized data by DML_Type 'I' for insert, 'U' for update or 'D' for
            delete.
        Optional[str]
            Dataset name if provided
    """
    data_to_archive: pd.DataFrame = source.copy()

    index: List[List[Any]] = [[row.__getattr__(c) for c in lookup_columns]
                              for _, row in data_to_archive.iterrows()]

    target_index: List[List[Any]] = [[row.__getattr__(c) for c in
                                      lookup_columns]
                                     for _, row in target.iterrows()]

    data_to_archive.loc[:, "DML_Type"] = data_to_archive.apply(
        lambda row: 'U' if index[row.name] in target_index else 'I',
        axis=1)

    try:
        data_to_archive = data_to_archive.append(
            pd.concat(
                [pd.DataFrame([row.to_list() + ['D']],
                              columns=list(row.to_dict().keys()) + ['DML_Type'])
                 for i, row in target.iterrows() if
                 [row.__getattr__(c) for c in lookup_columns]
                 not in index]),
            ignore_index=True)
    except ValueError:
        pass

    if name == '':
        return (data_to_archive,)
    else:
        return (data_to_archive, name)


def define_all_archives(
        source_target_lookup_columns_tuples: List[Tuple[pd.DataFrame,
                                                        List[str],
                                                        str]]
        ) -> Dict[str, pd.DataFrame]:
    """
    Categorize each source dataset by if lookup_column appears in associated target data.

    Makes multiple calls to define_archive for each source, target,
    lookup_column tuple in source_target_lookup_columns_tuples.

    Parameters
    ----------
    source_target_lookup_columns_tuples : List[Tuple[pd.DataFrame,
                                                     Listr[str],
                                                     str]
        Tuples of source data, associated target data, associated
        lookup_column(s), and name if desired.

    Returns
    -------
    Dict[str, pd.DataFrame]
        Dictionary of named dataframes categorized by DML_Type 'I' for insert,
        'U' for update, or 'D' for delete.
    """
    results: List[Tuple[pd.DataFrame, str]] = _client.gather(
        [_client.submit(define_archive,
                        source_target_lookup_columns_tuple[0],
                        source_target_lookup_columns_tuple[1],
                        source_target_lookup_columns_tuple[2],
                        source_target_lookup_columns_tuple[3])
         for source_target_lookup_columns_tuple
         in source_target_lookup_columns_tuples])
    return {r[1]: r[0] for r in results}


def split_update(archive_data: pd.DataFrame,
                 name: str = '') -> Tuple[pd.DataFrame,
                                          Optional[str]]:
    """
    Divide newly archived data into differentiated dataframes and a name.

    Parameters
    ----------
    archive_data : pd.DataFrame
        Archived data newly added to archive store.
    name : str
        Dataset name. Passed through if provided.

    Returns
    -------
    Tuple
        pd.DataFrame
            All rows from archive_data where DML_Type column is 'I'.
        pd.DataFrame
            All rows from archive_data where DML_Type column is 'U'.
        pd.DataFrame
            All rows from archive_data where DML_Type column is 'D'.
        Optional[str]
            Dataset name if provided
    """
    if name == '':
        return (
            archive_data.loc[archive_data.loc[
                :, 'DML_Type'] == 'I'].reset_index(drop=True).iloc[:, :-1],
            archive_data.loc[archive_data.loc[
                :, 'DML_Type'] == 'U'].reset_index(drop=True).iloc[:, :-1],
            archive_data.loc[archive_data.loc[
                :, 'DML_Type'] == 'D'].reset_index(drop=True).iloc[:, :-1])
    else:
        return (
            archive_data.loc[archive_data.loc[
                :, 'DML_Type'] == 'I'].reset_index(drop=True).iloc[:, :-1],
            archive_data.loc[archive_data.loc[
                :, 'DML_Type'] == 'U'].reset_index(drop=True).iloc[:, :-1],
            archive_data.loc[archive_data.loc[
                :, 'DML_Type'] == 'D'].reset_index(drop=True).iloc[:, :-1],
            name)


def split_all_updates(archive_datasets: Dict[str, pd.DataFrame]
                      ) -> List[Tuple[str, pd.DataFrame]]:
    """
    Split all newly archived datasets into differentiated DataFrames and a name.

    Parameters
    ----------
    archive_datasets : List[Tuple[pd.DataFrame, str]]
        Tuples of names and newly archived datasets.

    Returns
    -------
    List[Tuple[str, pd.DataFrame]]
        Tuples of dataset name, insert DataFrame, update DataFrame, and delete
        DataFrame.
    """
    results: List[Tuple[pd.DataFrame, str]] = _client.gather(
        [_client.submit(split_update, archive_dataset[1], archive_dataset[0])
         for archive_dataset in archive_datasets.items()])
    return [(r[-1], r[0], r[1], r[2]) for r in results]
