# -*- coding: utf-8 -*-

import sys
import os
import sqlite3
from datetime import date as dt_date

import nfetl

_default_db_path: str = os.path.join(os.path.dirname(sys.argv[0]), 'NFL.db')


class DB(object):

    def __init__(self, db_path: str = _default_db_path) -> None:
        """
        Base database object class.

        Parameters
        ----------
        db_path : str, optional
            Path where database will be located. The default is
            nfetl\\db\\NFL.db.
        """
        self._db_path = db_path
        self.__connection = sqlite3.connect(self.db_path)

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

    @property
    def connection(self) -> sqlite3.Connection:
        """
        sqlite3.Connection object
        Connection to created database. Change db_path to reset connection.
        """
        return self._connection

    @property
    def db_path(self) -> str:
        """
        str
        File path to database.
        """
        return self._db_path

    @db_path.setter
    def _set_db_path(self, new_path: str) -> None:
        self._db_path = new_path
        self._connection = sqlite3.connect(self._db_path)

    @property
    def last_update(self) -> dt_date:
        """
        datetime.date object
        Date that database was updated.
        """
        return self._last_update

    def update(self):
        """
        Extract all available data not currently in database, format, and load
        into database.
        """
        data = nfetl.extract.get_all()
        data = nfetl.transform.clean(data)
        nfetl.load.add_data(data)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        db = DB(sys.argv[1])
    else:
        db = DB()
    db.update()
