# -*- coding: utf-8 -*-


# %% Imports
# Py3 Standard
from typing import Tuple, List

# %%% 3rd Pary
import pandas as pd
import pytest

# %%% User Defined
from nfetl.processes.transform import (clean_data,
                                       clean_all_data,
                                       define_archive,
                                       define_all_archives,
                                       split_update,
                                       split_all_updates)


# %% Variables
source: pd.DataFrame = pd.DataFrame([[1, 1, 1],
                                     [1, 2, 2],
                                     [1, 2, 3],
                                     [1, 3, 4],
                                     [1, 3, 4]],
                                    columns=['1', '2', '3'])
source_2: pd.DataFrame = source.copy()
nk_columns: List[str] = ['2', '3']
nk_columns_2: List[str] = ['2']

expected_clean_data: pd.DataFrame = pd.DataFrame([[1, 1, 1],
                                                  [1, 2, 2],
                                                  [1, 2, 3],
                                                  [1, 3, 4]],
                                                 columns=['1', '2', '3'])
expected_dup_data: pd.DataFrame = pd.DataFrame([[1, 3, 4]],
                                               columns=['1', '2', '3'])

archive_target_data: pd.DataFrame = pd.DataFrame([[1, 1, 1],
                                                  [2, 2, 2],
                                                  [2, 4, 5],
                                                  [2, 5, 5]],
                                                 columns=['1', '2', '3'])
expected_archive_data: pd.DataFrame = pd.DataFrame(
    [[1, 1, 1, 'U'],
     [1, 2, 2, 'U'],
     [1, 2, 3, 'I'],
     [1, 3, 4, 'I'],
     [1, 3, 4, 'I'],
     [2, 4, 5, 'D'],
     [2, 5, 5, 'D']],
    columns=['1', '2', '3', 'DML_Type'])
expected_archive_data_2: pd.DataFrame = pd.DataFrame(
        [[1, 1, 1, 'U'],
         [1, 2, 2, 'U'],
         [1, 2, 3, 'U'],
         [1, 3, 4, 'U'],
         [1, 3, 4, 'U']],
        columns=['1', '2', '3', 'DML_Type'])

expected_split_data: Tuple[pd.DataFrame] = (
    pd.DataFrame([[1, 2, 3, 'I'],
                  [1, 3, 4, 'I'],
                  [1, 3, 4, 'I']],
                 columns=['1', '2', '3', 'DML_Type']),
    pd.DataFrame([[1, 1, 1, 'U'],
                  [1, 2, 2, 'U']],
                 columns=['1', '2', '3', 'DML_Type']),
    pd.DataFrame([[2, 4, 5, 'D'],
                  [2, 5, 5, 'D']],
                 columns=['1', '2', '3', 'DML_Type']))


# %% Functions
def test_clean_data() -> None:
    global source
    global nk_columns
    global expected_clean_data
    global expected_dup_data

    results: Tuple[pd.DataFrame, pd.DataFrame] = clean_data(source, nk_columns)

    assert results[0].equals(expected_clean_data) and \
        results[1].equals(expected_dup_data)

def test_clean_all_data() -> None:
    global source
    global source_2
    global nk_columns
    global nk_columns_2
    global expected_clean_data
    global expected_dup_data

    expected_clean_data_2: pd.DataFrame = pd.DataFrame([[1, 1, 1],
                                                        [1, 2, 2],
                                                        [1, 3, 4]],
                                                       columns=['1', '2', '3'])
    expected_dup_data_2: pd.DataFrame = pd.DataFrame([[1, 2, 3],
                                                      [1, 3, 4]],
                                                     columns=['1', '2', '3'])

    results: List[Tuple[pd.DataFrame, pd.DataFrame]] = clean_all_data(
        [(source, nk_columns, 'one'),
         (source_2, nk_columns_2, 'two')])
    results_1 = results[0] if results[0][-1] == 'one' else results[1]
    results_2 = results[1] if results[1][-1] == 'two' else results[0]

    assert results_1[0].equals(
        expected_clean_data) and results_1[1].equals(
            expected_dup_data) and results_2[0].equals(
                expected_clean_data_2) and results_2[1].equals(
                    expected_dup_data_2)

def test_define_archive() -> None:
    global source
    global archive_target_data
    global nk_columns
    global expected_archive_data

    results: pd.DataFrame = define_archive(source,
                                           archive_target_data,
                                           nk_columns)[0]

    assert results.equals(expected_archive_data)

def test_define_all_archives() -> None:
    global source
    global source_2
    global nk_columns
    global nk_columns_2
    global archive_target_data
    global expected_archive_data
    global expected_archive_data_2

    archive_target_data_2: pd.DataFrame = pd.DataFrame([[1, 1, 1],
                                                        [1, 2, 2],
                                                        [1, 2, 3],
                                                        [1, 3, 4]],
                                                       columns=['1', '2', '3'])

    results: List[Tuple[pd.DataFrame]] = define_all_archives(
        [(source, archive_target_data, nk_columns, 'data'),
         (source_2, archive_target_data_2, nk_columns_2, 'data_2')])
    results_1 = results[0] if results[0][-1] == 'data' else results[1]
    results_2 = results[1] if results[1][-1] == 'data_2' else results[0]

    assert results_1[0].equals(
        expected_archive_data) and results_2[0].equals(
            expected_archive_data_2)

def test_split_update() -> None:
    global expected_archive_data
    global expected_split_data

    results: Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] = split_update(
        expected_archive_data)

    assert results[0].equals(
        expected_split_data[0]) and results[1].equals(
            expected_split_data[1]) and results[2].equals(
                expected_split_data[2])

def test_split_all_updates() -> None:
    global expected_archive_data
    global expected_archive_data_2
    global expected_split_data

    expected_split_data_2: Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] = (
        pd.DataFrame(columns=['1', '2', '3', 'DML_Type']).astype(
            {'1': 'int64', '2': 'int64', '3': 'int64', 'DML_Type': 'object'}),
        pd.DataFrame([[1, 1, 1, 'U'],
                      [1, 2, 2, 'U'],
                      [1, 2, 3, 'U'],
                      [1, 3, 4, 'U'],
                      [1, 3, 4, 'U']],
                     columns=['1', '2', '3', 'DML_Type']),
        pd.DataFrame(columns=['1', '2', '3', 'DML_Type']).astype(
            {'1': 'int64', '2': 'int64', '3': 'int64', 'DML_Type': 'object'}))

    results: List[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]] = \
        split_all_updates(
            [(expected_archive_data, 'one'),
             (expected_archive_data_2, 'two')])
    results_1 = results[0] if results[0][-1] == 'one' else results[1]
    results_2 = results[1] if results[1][-1] == 'two' else results[0]

    assert results_1[0].equals(
        expected_split_data[0]) and results_1[1].equals(
            expected_split_data[1]) and results_1[2].equals(
                expected_split_data[2]) and results_2[0].equals(
                    expected_split_data_2[0]) and results_2[1].equals(
                        expected_split_data_2[1]) and results_2[2].equals(
                            expected_split_data_2[2])
