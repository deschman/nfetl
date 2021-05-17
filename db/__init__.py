# -*- coding: utf-8 -*-
"""Module defining internal database object, DB."""


# %% Imports
# %%% Py3 Standard
import sys
import os
import sqlite3
from datetime import date
from typing import Dict

# %%% 3rd Party
import pandas as pd

# %%% User Defined
from nfetl.processes import extract, transform, load


# %% Variables
# %%% System
__all__ = ['DB']

# %%% Private
_default_db_path: str = os.path.join(os.path.dirname(sys.argv[0]), 'NFL.db')


# %% Classes
class DB(object):
    r"""
    Base database object class.

    Parameters
    ----------
    db_path : str, optional
        Path where database will be located. The default is nfetl\db\NFL.db.
    """

    # %% Functions
    # %%% Private
    def __init__(self, db_path: str = _default_db_path) -> None:
        self._db_path = db_path
        self.__connection = sqlite3.connect(
            self.db_path, check_same_thread=False)

    @property
    def _connection(self) -> sqlite3.Connection:
        return self.__connection

    @_connection.setter
    def _set_connection(self, new_connection: sqlite3.Connection) -> None:
        if self.__connection is not None:
            self.__connection.close()
        self.__connection = new_connection

    @_connection.deleter
    def _del_connection(self) -> None:
        self.__connection.close()

    # %%% Public
    @property
    def connection(self) -> sqlite3.Connection:
        """
        Get connection property.

        Returns
        -------
        sqlite3.Connection object
            Connection to database. If db_path property is changed, this
            connection will be reset.
        """
        return self._connection

    @property
    def db_path(self) -> str:
        """
        Get db_path property.

        Returns
        -------
        str
            File path to database.
        """
        return self._db_path

    @db_path.setter
    def _set_db_path(self, new_path: str) -> None:
        self._db_path = new_path
        self._connection = sqlite3.connect(self._db_path)

    @property
    def last_update(self) -> date:
        """
        Get last_update property.

        date object
            Date that database was updated.
        """
        return self._last_update

    def land_all_data(self, dfs: Dict[str, pd.DataFrame]) -> None:
        __doc__ = load.land_all_data.__doc__
        load.land_all_data(self, dfs)

    def update(self) -> None:
        """Extract and load all data not currently in database."""
        data: Dict[str, pd.DataFrame] = extract.get_update()
        self.land_all_data(data)
        # transform.clean_all_data()
        # transform.define_all_archive()
        # transform.split_all_update()


# %% Script
if __name__ == '__main__':
    if len(sys.argv) > 1:
        db = DB(sys.argv[1])
    else:
        db = DB()
    db.update()
