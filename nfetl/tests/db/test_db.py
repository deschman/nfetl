# -*- coding: utf-8 -*-
"""Test functionality of nfetl.db.DB."""


# %% Imports
# %%% Py3 Standard
import os
from typing import Dict, Tuple, List

# %%% 3rd Party
import pytest
import pandas as pd

# %%% User-Defined
from nfetl import DB
from nfetl.core import _config
from nfetl._datetime import _date


# %% Variables
db: DB = DB(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'NFL.db'),
            False)

source: pd.DataFrame = pd.read_hdf(
    _config['DEFAULT']['test_url_data'], 'test_url_data')
clean_source: pd.DataFrame = pd.read_hdf(
    _config['DEFAULT']['test_url_data'], 'test_clean_data')
section_name: str = [i for i in _config.sections() if 'test' in i][0]
source_table_name: str = section_name + '_2019'
extracted_data: Dict[str, pd.DataFrame] = {source_table_name: source}
archive: pd.DataFrame = source.copy()
archive.insert(len(archive.columns), 'DML_Type', 'I')
arc_data: Dict[str, pd.DataFrame] = {f'{source_table_name}': archive}


# %% Functions
# %%% Private
def _truncate_table(prefix: str,
                    dfs: Dict[str, pd.DataFrame] = extracted_data) -> None:
    for table in extracted_data.keys():
        if pd.read_sql(
                f"SELECT COUNT(1) FROM sqlite_master WHERE name = '{prefix}{table}'",
                db.connection).iat[0, 0] > 0:
            db.connection.execute(f"DELETE FROM {prefix}{table}")


# %%% Public
def test__strip_prefix() -> None:
    assert source_table_name == db._strip_prefix(f'LND_{source_table_name}')


def test__strip_year() -> None:
    assert section_name == db._strip_year(source_table_name)


def test__create_views() -> None:
    db._create_views()
    try:
        assert section_name in db.views
    except IndexError:
        assert True


def test_connection() -> None:
    assert len(db.connection.execute(
        "SELECT COUNT(1) FROM sqlite_master").fetchall()) > 0


def test_db_path() -> None:
    assert db.db_path == os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        'NFL.db')


def test_tables() -> None:
    assert all([t in db.tables for t in ['ARC_test_2019',
                                         'CUR_test_2019',
                                         'DUP_test_2019',
                                         'LND_test_2019',
                                         'LND_TEST_LANDING']])


def test_tables_and_views() -> None:
    assert db.tables_and_views == db.tables + db.views


def test_views() -> None:
    assert 'test' in db.views and isinstance(db.views, list)


def test_last_update() -> None:
    assert type(db.last_update) == int


def test_load_all_lnd_data() -> None:
    _truncate_table('LND_')
    db.load_all_lnd_data(extracted_data)

    assert pd.read_sql(
        f"SELECT * FROM LND_{source_table_name}",
        db.connection).iloc[:, 1:].equals(
            extracted_data[source_table_name])


def test_clean_all_data() -> None:
    clean_data: Tuple[Dict[str, pd.DataFrame]] = db.clean_all_data(
        extracted_data)

    assert (clean_data[0][source_table_name].equals(
        extracted_data[source_table_name]) and
        clean_data[1][source_table_name].empty)


def test_load_all_dup_data() -> None:
    _truncate_table('DUP_')
    db.load_all_dup_data(extracted_data)
    db.load_all_dup_data(
        {source_table_name: pd.DataFrame(
            columns=source.columns.to_list())})

    load_result: pd.DataFrame = pd.read_sql(
        f"SELECT * FROM DUP_{source_table_name}", db.connection).iloc[:, 1:]
    assert load_result.empty and (
        load_result.columns == extracted_data[source_table_name].columns).all()


def test_define_all_archives() -> None:
    _truncate_table('CUR_')
    results: Tuple[str, pd.DataFrame] = list(
        db.define_all_archives(extracted_data).items())[0]

    assert results[0] == source_table_name and results[1].equals(
        arc_data[source_table_name])


def test_load_all_arc_data() -> None:
    _truncate_table('ARC_')
    db.load_all_arc_data(arc_data)

    load_result: pd.DataFrame = pd.read_sql(
        f"SELECT * FROM ARC_{source_table_name}", db.connection)
    assert load_result.iloc[:, 1:].equals(arc_data[source_table_name])


def test_split_all_updates() -> None:
    results: List[Tuple[str, pd.DataFrame]] = db.split_all_updates(arc_data)[0]

    assert (results[0] == source_table_name and
            results[1].equals(archive.iloc[:, :-1]) and
            results[2].empty and
            results[3].empty)


def test_load_all_cur_data() -> None:
    _truncate_table('CUR_')
    cur_data: pd.DataFrame = archive.iloc[:, :-1]
    db.load_all_cur_data([(source_table_name,
                           cur_data,
                           pd.DataFrame(columns=cur_data.columns),
                           pd.DataFrame(columns=cur_data.columns))])

    load_result: pd.DataFrame = pd.read_sql(
        f"SELECT * FROM CUR_{source_table_name}", db.connection).iloc[:, 1:]
    assert load_result.equals(extracted_data[source_table_name])


def test_update() -> None:
    year: int = _date.today().nfl_year - 1

    db.update(year)

    assert all([t in db.views for t in _config.sections() if t != section_name])
