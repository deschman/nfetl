# -*- coding: utf-8 -*-
"""Test load functions in nfetl.processes.load."""


# %% Imports
# %%% Py3 Standard
import os
import time
from datetime import datetime

# %%% 3rd Party
import pandas as pd

# %%% User Defined
from nfetl.db import DB
from nfetl.processes.load import (_timestamp_data,
                                  _select_data_subset,
                                  load_data,
                                  load_multiple,
                                  update_data,
                                  delete_data,
                                  process_all_updates)


# %% Variables
db: DB = DB(False,
            os.path.join(os.path.dirname(os.path.dirname(__file__)), 'NFL.db'))
df: pd.DataFrame = pd.DataFrame([['1', '2'], ['2', '3']], columns=['1', '2'])


# %% Functions
def test__timestamp_data() -> None:
    test_stamp_1: str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    time.sleep(1)
    df_stamp: str = _timestamp_data(df).iat[0, 0]
    time.sleep(1)
    test_stamp_2: str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    assert test_stamp_2 > df_stamp and df_stamp > test_stamp_1


def test__select_data_subset() -> None:
    load_data(db, 'LND_TEST_LANDING', df)
    df2: pd.DataFrame = _select_data_subset(db,
                                            'LND_TEST_LANDING',
                                            pd.DataFrame([['1']], columns=['1']),
                                            ['1'])
    assert df2.loc[0, '1'] == '2'


def test_load_data() -> None:
    db.connection.execute("DELETE FROM LND_TEST_LANDING")
    load_data(db, 'LND_TEST_LANDING', df)
    assert '1' == pd.read_sql("SELECT * FROM LND_TEST_LANDING",
                              db.connection).loc[0, '1']


def test_load_multiple() -> None:
    df_2: pd.DataFrame = pd.DataFrame(['0'], columns=['0'])
    load_multiple(db, {'TEST_LANDING': df, 'TEST_LANDING_2': df_2}, 'LND_')
    assert '1' == pd.read_sql(
        "SELECT * FROM LND_TEST_LANDING",
        db.connection).loc[0, '1'] and '0' == pd.read_sql(
            "SELECT * FROM LND_TEST_LANDING_2",
            db.connection).loc[0, '0']


def test_update_data() -> None:
    load_data(db, 'LND_TEST_LANDING', df)
    update_data(db,
                'LND_TEST_LANDING',
                pd.DataFrame([['2', '2']], columns=['1', '2']),
                ['1'])
    assert '2' == pd.read_sql(
        "SELECT * FROM LND_TEST_LANDING",
        db.connection).loc[1, '2']


def test_delete_data() -> None:
    load_data(db, 'LND_TEST_LANDING', df)
    delete_data(db,
                'LND_TEST_LANDING',
                pd.DataFrame([['2', '3']], columns=['1', '2']),
                ['1'])
    assert len(
        pd.read_sql("SELECT * FROM LND_TEST_LANDING", db.connection)) == 1


def test_process_all_updates() -> None:
    load_data(db, 'LND_TEST_LANDING', df)
    process_all_updates(db,
                        [('LND_TEST_LANDING',
                          pd.DataFrame([['3', '3']], columns=['1', '2']),
                          pd.DataFrame([['1', '1']], columns=['1', '2']),
                          pd.DataFrame([['2', '3']], columns=['1', '2']),
                          ['1'])])
    assert pd.DataFrame([['3', '3'], ['1', '1']], columns=['1', '2']).equals(
        pd.read_sql("SELECT * FROM LND_TEST_LANDING",
                    db.connection).loc[:, ['1', '2']])
