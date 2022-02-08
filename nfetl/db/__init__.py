# -*- coding: utf-8 -*-
"""Module defining internal database object, DB."""


# %% Imports
# %%% Py3 Standard
import os
import sqlite3
from typing import Dict, Tuple, List

# %%% 3rd Party
import pandas as pd

# %%% User Defined
from nfetl._datetime import _date
from nfetl.core import _config, _argparser, _DB
from nfetl.processes import extract as _extract
from nfetl.processes import transform as _transform
from nfetl.processes import load as _load


# %% Variables
# %%% System
__all__ = ['DB']

# %%% Private
_default_db_path: str = os.path.join(os.path.dirname(__file__), 'NFL.db')
_default_start_year: int = int(_config['DEFAULT']['start_year'])


# %% Classes
class DB(_DB):
    r"""
    Base database object class.

    Parameters
    ----------
    db_path : str, optional
        Path where database will be located. The default is nfetl\db\NFL.db.
    auto_update : bool, default is False
        If True, database will update, adding any new data.
    update_start : int, optional
        Year that update will start.
    update_end : int, optional
        Year that updated will end.
    """

    # %% Functions
    # %%% Private
    def __init__(self,
                 db_path: str = _default_db_path,
                 auto_update: bool = True,
                 update_start: int = -1,
                 update_end: int = -1) -> None:
        self._db_path: str = db_path
        self.__connection: sqlite3.Connection = sqlite3.connect(
            self.db_path, check_same_thread=False)
        if auto_update and self.last_update <= _date.today().nfl_year:
            self.update(update_start, update_end)

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

    def _strip_prefix(self, name: str) -> str:
        """
        Remove one of ('LND_', 'DUP_', 'ARC_', 'CUR_') from name.

        Parameters
        ----------
        name : str
            Table name where prefix will be stripped.

        Returns
        -------
        str
            name after prefix has been stripped.
        """
        return name[4:] if any(
            [True for s in ['LND_', 'DUP_', 'ARC_', 'CUR_'] if s in name]
            ) else name

    def _strip_year(self, name: str) -> str:
        """
        Remove last part of name if it is numeric,.

        Parameters
        ----------
        name : str
            Name to have year stripped.

        Returns
        -------
        str
            name with last_part stripped if it was numeric.
        """
        last_part: str = name.split('_')[-1]
        return name[:-len(last_part) - 1] if last_part.isnumeric() else name

    def _query_target_data(self, table: str) -> pd.DataFrame:
        """
        Get current data for one table.

        Parameters
        ----------
        table : str
            Table name.

        Returns
        -------
        pd.DataFrame
            Current data for table.
        """
        return pd.read_sql(f"SELECT * FROM CUR_{table}", self.connection)

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
        """
        Set db_path property.

        Parameters
        ----------
        new_path : str
            New file path to database.
        """
        self._db_path = new_path
        self._connection = sqlite3.connect(self._db_path)

    @property
    def last_update(self) -> int:
        """
        Identify nfl_year of last update.

        int
            nfl_year that database was last updated.
        """
        try:
            return max([int(t.split('_')[-1]) for t in self.tables
                        if t.split('_')[-1].isnumeric()])
        except ValueError:
            return _default_start_year

    @property
    def tables(self) -> List[str]:
        """
        List tables in db.

        Returns
        -------
        List[str]
            List of all tables in db.
        """
        try:
            return pd.read_sql(
                "SELECT name FROM sqlite_master WHERE type = 'table'",
                self.connection).iloc[:, 0].to_list()
        except IndexError:
            return []

    @property
    def tables_and_views(self) -> List[str]:
        """
        List all tables and views in db.

        Returns
        -------
        List[str]
            List of all tables and views in db.
        """
        return self.tables + self.views

    @property
    def views(self) -> List[str]:
        """
        List views in db.

        Returns
        -------
        List[str]
            List of all views in db.
        """
        try:
            return pd.read_sql(
                "SELECT name FROM sqlite_master WHERE type = 'view'",
                self.connection).iloc[:, 0].to_list()
        except IndexError:
            return []

    def _create_views(self) -> None:
        """Create a view for each subject area containing all yearly data."""

        def _extend_ddl(name: str) -> str:
            """
            Extend DDL for view to include table name.

            Parameters
            ----------
            name : str
                Name of table.

            Returns
            -------
            str
                DDL extension.
            """
            base_name: str = self._strip_year(self._strip_prefix(name))
            extract_columns: List[str] = _config[base_name]['extract_columns'].split(', ')
            transform_columns: List[str] = _config[base_name]['transform_columns'].split(', ')
            year: str = name.split('_')[-1]
            select_columns: str = year + ' AS "Year", ' + str(
                ['"' + extract_columns[i] + '" AS "' + transform_columns[i] + '"'
                 for i in range(len(extract_columns))])[1:-1].replace("'", '')

            return f"\nUNION ALL\nSELECT\n\t{select_columns}\nFROM {name}"

        def _drop_and_create_view(row: pd.core.frame.Series) -> None:
            self.connection.execute(f"DROP VIEW IF EXISTS {row.group_name}")
            self.connection.execute(
                f"""CREATE VIEW {row.group_name} AS
                {''.join([_extend_ddl(s) for s in
                          row.table_list.split(', ')])[11:]}""")

        groups_and_tables: pd.DataFrame = pd.DataFrame(
            list(set(
                [self._strip_year(self._strip_prefix(i)) for i in self.tables])),
            columns=['group_name'])
        groups_and_tables.loc[:, 'table_list'] = groups_and_tables.apply(
            lambda r: str(
                [t for t in self.tables if r.group_name in t and 'CUR_' in t]
                )[1:-1].replace("'", ''),
            axis=1)

        [_drop_and_create_view(r) for r in
         groups_and_tables.loc[
             groups_and_tables.iloc[:, 1] != ''].itertuples()]

    def load_all_lnd_data(self, dfs: Dict[str, pd.DataFrame]) -> None:
        """
        Load all landing data in dictionary of named DataFrames.

        Parameters
        ----------
        dfs : Dict[str, pd.DataFrame]
            Dicitonary of named DataFrames.
        """
        _load.load_multiple(self, dfs, 'LND_', True)

        def _create_staging_tables(name: str) -> None:
            """
            Copy table without data to create staging area.

            Parameters
            ----------
            name : str
                Base table name.
            """
            try:
                self.connection.execute(
                    f"CREATE TABLE DUP_{name} AS SELECT * FROM LND_{name}")
            except sqlite3.OperationalError:
                # If table already exists, truncate
                self.connection.execute(f"DELETE FROM DUP_{name}")
            try:
                self.connection.execute(
                    f"CREATE TABLE ARC_{name} AS SELECT * FROM LND_{name}")
                self.connection.execute(
                    f"ALTER TABLE ARC_{name} ADD COLUMN DML_TYPE TEXT")
            except sqlite3.OperationalError:
                # If table already exists, ignore
                pass
            try:
                self.connection.execute(
                    f"CREATE TABLE CUR_{name} AS SELECT * FROM LND_{name}")
            except sqlite3.OperationalError:
                # If table already exists, ignore
                pass
        [_create_staging_tables(k) for k in dfs.keys()]

    def clean_all_data(self,
                       dfs: Dict[str, pd.DataFrame]
                       ) -> Tuple[Dict[str, pd.DataFrame]]:
        """
        De-duplicate data to a de-duplicated df and a df of duplicates.

        Parameters
        ----------
        dfs : Dict[str, pd.DataFrame]
            Dictionary of named DataFrames.

        Returns
        -------
        Tuple[Dict[str, pd.DataFrame]]
            De-duplicated data dictionary and duplicate data dictionary in a
            tuple.
        """
        return _transform.clean_all_data(
            [(i[1],
              _config[self._strip_year(i[0])]['nk_columns'].split(', '),
              i[0])
             for i in dfs.items()])

    def load_all_dup_data(self, dfs: Dict[str, pd.DataFrame]) -> None:
        """
        Load all duplicate data in dictionary of named DataFrames.

        Parameters
        ----------
        dfs : Dict[str, pd.DataFrame]
            Dicitonary of named DataFrames.
        """
        _load.load_multiple(self, dfs, 'DUP_')

    def define_all_archives(
            self,
            dfs: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, str]:
        """
        Define each row of included dataframes as an insert, update, or delete.

        Parameters
        ----------
        dfs : Dict[str, pd.DataFrame]
            Dictionary of named DataFrames.
        """
        return _transform.define_all_archives(
            [(i[1],
              self._query_target_data(i[0]),
              _config[self._strip_year(i[0])]['nk_columns'].split(', '),
              i[0]) for i in dfs.items()])

    def load_all_arc_data(self, dfs: Dict[str, pd.DataFrame]) -> None:
        """
        Load all archive data in dictionary of named DataFrames.

        Parameters
        ----------
        dfs : Dict[str, pd.DataFrame]
            Dicitonary of named DataFrames.
        """
        _load.load_multiple(self, dfs, 'ARC_')

    def split_all_updates(self,
                          dfs: Dict[str, pd.DataFrame]
                          ) -> List[Tuple[str, pd.DataFrame]]:
        """
        Split data formatted as archive into insert, update, and delete data.

        Parameters
        ----------
        dfs : Dict[str, pd.DataFrame]
            Dictionary of named DataFrames.

        Returns
        -------
        List[Tuple[str, pd.DataFrame]]
            Dataset name, insert data, update data, and delete data
        """
        return _transform.split_all_updates(dfs)

    def load_all_cur_data(self,
                          dfs: List[Tuple[str, pd.DataFrame]]) -> None:
        """
        Update current data with inserts, updates, and deletes.

        Parameters
        ----------
        dfs : List[Tuple[str, pd.DataFrame]]
            Each tuple should contain dataset name, an insert DataFrame, an
            update DataFrame, and a delete DataFrame.
        """
        dfs = [('CUR_' + i[0],
                i[1],
                i[2],
                i[3],
                _config[self._strip_year(i[0])]['nk_columns'].split(', '))
               for i in dfs]
        _load.process_all_updates(self, dfs)

        self._create_views()

    def update(self, start_year: int = -1, end_year: int = -1) -> None:
        """
        Extract and load all data not currently in database.

        Parameters
        ----------
        start_year : int, default is 1970
            Year in which to start update.
        end_year : int, default is current year.
            Year in which update will stop
        """
        start_year = self.last_update if start_year == -1 else start_year
        end_year = _date.today().nfl_year + 1 if end_year == -1 else end_year

        for y in range(start_year, end_year):
            landing_data: Dict[str, pd.DataFrame] = _extract.get_update(y)
            self.load_all_lnd_data(landing_data)

            clean_results: Tuple[pd.DataFrame] = self.clean_all_data(
                landing_data)
            dup_data: pd.DataFrame = clean_results[1]
            self.load_all_dup_data(dup_data)
            clean_data: pd.DataFrame = clean_results[0]
            del clean_results

            archive_data: pd.DataFrame = self.define_all_archives(clean_data)
            self.load_all_arc_data(archive_data)

            update_data: List[Tuple[str,
                                    pd.DataFrame]] = self.split_all_updates(
                archive_data)
            self.load_all_cur_data(update_data)
