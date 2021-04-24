# -*- coding: utf-8 -*-
"""Test load functions in nfetl.processes.load."""


# %% Imports
# %%% Py3 Standard
import time
from datetime import datetime

# %%% 3rd Party
import pandas as pd

# %%% User Defined
from nfetl.db import DB
from nfetl.processes.load import (_timestamp_data,
                                  land_data,
                                  land_all_data)


# %% Variables
db: DB = DB()
df: pd.DataFrame = pd.DataFrame(['1'], columns=['1'])


# %% Functions
def test__timestamp_data() -> None:
    test_stamp_1: str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    time.sleep(1)
    df_stamp: str = _timestamp_data(df).iat[0, 0]
    time.sleep(1)
    test_stamp_2: str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    assert test_stamp_2 > df_stamp and df_stamp > test_stamp_1

def test_land_data() -> None:
    land_data(db, 'TEST_LANDING', df)
    assert '1' == pd.read_sql("SELECT * FROM LND_TEST_LANDING",
                              db.connection).loc[0, '1']

def test_land_all_data() -> None:
    df_2: pd.DataFrame = pd.DataFrame(['0'], columns=['0'])
    land_all_data(db, {'TEST_LANDING_1': df, 'TEST_LANDING_2': df_2})
    assert '1' == pd.read_sql(
        "SELECT * FROM LND_TEST_LANDING_1",
        db.connection).loc[0, '1'] and '0' == pd.read_sql(
            "SELECT * FROM LND_TEST_LANDING_2",
            db.connection).loc[0, '0']


# %% Script
if __name__ == '__main__':
    test__timestamp_data()
    test_land_data()
    test_land_all_data()
