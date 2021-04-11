# -*- coding: utf-8 -*-
"""Extract data from pro-football-reference.com."""


# %% Imports
# %%% Py3 Standard
from random import randrange, random
from time import sleep
import urllib.request
from urllib.error import URLError
from typing import List, Tuple, Dict

# %%% 3rd Party
from bs4 import BeautifulSoup
import pandas as pd
from dask import distributed

# %%% User Defined
from nfetl._config import _config as config
from nfetl._datetime import date


# %% Variables
# %%% System
__all__ = ['get_url_data', 'get_update']

# %%% Private
_client: object = distributed.Client(processes=False)

_default_start_year: int = int(config['Scope']['start_year'])
_default_sets: List[Tuple[str, str, str]] = [
    (key, config['URLs'][key], config['Columns_extract'][key])
    for key in config['Scope']['keys'].split(', ')]


# %% Functions
def get_url_data(url: str, headers: List[str] = []) -> pd.DataFrame:
    """
    Retrieve data table from URL.

    Parameters
    ----------
    url : str
        URL for HTML page where data table is found.
    headers : List[str], optional
        Headers for retrieved table. Default is source headers.

    Returns
    -------
    data : pandas.DataFrame
        Data table from HTML page found at URL.
    """
    try:
        html: object = urllib.request.urlopen(url)
    except URLError:
        return pd.DataFrame()
    table: List[str] = BeautifulSoup(html, 'html.parser').findAll('tr')
    # Reduce table size if more than one table is present in data
    i: int = 0
    for row in table:
        try:
            if i > 1 and 'thead' not in row.attrs.get('class')[0]:
                table = table[:i]
                break
        except TypeError:
            pass
        finally:
            i += 1
    # sub_table tracks the subsection being retrieved
    sub_table: List[str] = table
    # remaining_table tracks the table sections yet to be retrieved
    remaining_table: List[str] = table
    for header_row in [1, 0]:
        if len(headers) < 2:
            headers = [i.getText() for i in table[header_row].findAll('th')]
    data: pd.DataFrame = pd.DataFrame(columns=headers)
    # variables for building sub_table, remaining table
    done: bool = False
    started: bool = False
    i = 0
    b: int = i
    while not done:
        try:
            data = data.append(
                pd.DataFrame([max([c.getText() for c in row],
                                  ['']*len(headers)) for row in sub_table],
                             columns=headers))
            remaining_table = remaining_table[i:]
            sub_table = remaining_table
            if len(remaining_table) == 0:
                done = True
        except (ValueError, AttributeError) as e:
            if isinstance(e, AttributeError):
                i = 0
                b = 0
                for row in sub_table:
                    i += 1
                    if any([not hasattr(c, 'getText') for c in row]):
                        if started and i > b + 1:
                            i -= 1
                            break
                        else:
                            b = i
                    else:
                        if not started:
                            started = True
                sub_table = sub_table[b:i]
            else:
                i = 0
                for row in sub_table:
                    if len(max([c.getText() for c in row],
                               ['']*len(headers))) == len(headers):
                        i += 1
                    else:
                        continue
                sub_table = sub_table[:i]
    return data


def get_update(start_year: int = _default_start_year,
               datasets: List[Tuple[str, str, str]] = _default_sets):
    """
    Retrieve all data tables from URL list provided by user or in config file.

    Makes multiple calls to get_url_data from the dask distributed client. Will
    retrieve data from start_year and after.

    Parameters
    ----------
    start_year : int, optional
        Four digit year for start of update. Default is from config file.
    datasets : List[Tuple[str, str]], optional
        Contains URL strings and header lists from user input or config file.
        Default are from config file.

    Returns
    -------
    dfs : list
        Contains pandas.DataFrame objects containing data from table found on
        HTML page located at URL.
    """
    if start_year > date.today().nfl_year:
        raise Exception
    dfs: Dict[str, pd.DataFrame] = {}
    headers: List[str] = []
    url: str = ''
    for dataset in datasets:
        headers = dataset[2].split(', ')
        for y in range(start_year, date.today().nfl_year + 1):
            url = dataset[1].format(str(y))
            dfs[dataset[0]] = _client.submit(get_url_data, url=url, headers=headers)
            sleep(randrange(3, 6) + random())
    distributed.wait(dfs.values())
    dfs = {v[0]: v[1].result() for v in dfs.items()}
    return dfs
