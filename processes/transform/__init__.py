# -*- coding: utf-8 -*-
"""Transform data sourced from pro-football-reference.com."""


# %% Imports
# %%% Py3 Standard
from typing import List, Tuple, Optional, Any

# %%% 3rd Party
import pandas as pd
from dask import distributed


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
               name: str = '') -> (pd.DataFrame, pd.DataFrame, Optional[str]):
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
    clean_data: pd.DataFrame = source.drop_duplicates(nk_columns)
    removed_data: pd.DataFrame = source.loc[~source.index.isin(
        clean_data.index)].reset_index(drop=True)
    clean_data.reset_index(drop=True, inplace=True)
    if name == '':
        return (clean_data, removed_data)
    else:
        return (clean_data, removed_data, name)

def clean_all_data(sources_nk_tuples: List[Tuple[pd.DataFrame, List[str], str]]
                   ) -> List[Tuple[pd.DataFrame, pd.DataFrame, str]]:
    """
    Validate all source data contains no violations of nk_columns.

    Makes multiple calls to clean_data for each name, source, nk_columns tuple
    in sources_nk_tuples list.

    Parameters
    ----------
    sources_nk_tuples : List[Tuple[pd.DataFrame, List[str], str]]
        Tuples of source data name, data, and accompanying nk_column list.

    Returns
    -------
    List[Tuple[pd.DataFrame, pd.DataFrame, str]]
        Tuples of name, data with key violations removed, and data with only
        removed key violations.

    """
    return _client.gather([_client.submit(clean_data,
                                          source_nk_tuple[0],
                                          source_nk_tuple[1],
                                          source_nk_tuple[2])
        for source_nk_tuple in sources_nk_tuples])

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
    pd.DataFrame
        Categorized data by DML_Type 'I' for insert, 'U' for update or 'D' for
        delete.
    Optional[str]
        Dataset name if provided
    """
    data_to_archive: pd.DataFrame = source.copy()

    index: List[List[Any]] = [[row.__getattr__(c) for c in lookup_columns]
                              for i, row in data_to_archive.iterrows()]

    target_index: List[List[Any]] = [[row.__getattr__(c) for c in
                                      lookup_columns]
                                     for i, row in target.iterrows()]

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
                                                        pd.DataFrame,
                                                        List[str],
                                                        str]]
        ) -> List[Tuple[pd.DataFrame, Optional[str]]]:
    """
    Categorize each source dataset by if lookup_column appears in associated target data.

    Makes multiple calls to define_archive for each source, target,
    lookup_column tuple in source_target_lookup_columns_tuples.

    Parameters
    ----------
    source_target_lookup_columns_tuples : List[Tuple[pd.DataFrame,
                                                     pd.DataFrame,
                                                     Listr[str],
                                                     str]
        Tuples of source data, associated target data, associated
        lookup_column(s), and name if desired.

    Returns
    -------
    List[Tuple[pd.DataFrame, str]]
        Tuples of name and dataframe categorized by DML_Type 'I' for insert,
        'U' for update, or 'D' for delete.
    """
    return _client.gather(
        [_client.submit(define_archive,
                        source_target_lookup_columns_tuple[0],
                        source_target_lookup_columns_tuple[1],
                        source_target_lookup_columns_tuple[2],
                        source_target_lookup_columns_tuple[3])
         for source_target_lookup_columns_tuple
         in source_target_lookup_columns_tuples])

def split_update(archive_data: pd.DataFrame,
                 name: str = '') -> Tuple[pd.DataFrame,
                                          pd.DataFrame,
                                          pd.DataFrame,
                                          Optional[str]]:
    """
    Split newly archived data into differentiated dataframes and a name.

    Parameters
    ----------
    archive_data : pd.DataFrame
        Archived data newly added to archive store.
    name : str
        Dataset name. Passed through if provided.

    Returns
    -------
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
        return (archive_data.loc[archive_data.loc[
                    :, 'DML_Type'] == 'I'].reset_index(drop=True),
                archive_data.loc[archive_data.loc[
                    :, 'DML_Type'] == 'U'].reset_index(drop=True),
                archive_data.loc[archive_data.loc[
                    :, 'DML_Type'] == 'D'].reset_index(drop=True))
    else:
        return (archive_data.loc[archive_data.loc[
                    :, 'DML_Type'] == 'I'].reset_index(drop=True),
                archive_data.loc[archive_data.loc[
                    :, 'DML_Type'] == 'U'].reset_index(drop=True),
                archive_data.loc[archive_data.loc[
                    :, 'DML_Type'] == 'D'].reset_index(drop=True),
                name)

def split_all_updates(archive_datasets: List[Tuple[pd.DataFrame, str]]
                      ) -> List[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]]:
    """
    Split all newly archived datasets into differentiated DataFrames and a name.

    Parameters
    ----------
    archive_datasets : List[Tuple[pd.DataFrame, str]]
        Tuples of names and newly archived datasets.

    Returns
    -------
    List[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]]
        Tuples of names, insert DataFrame, and update DataFrame.
    """
    return _client.gather(
        [_client.submit(split_update, archive_dataset[0], archive_dataset[1])
         for archive_dataset in archive_datasets])
