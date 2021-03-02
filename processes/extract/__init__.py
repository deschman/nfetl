# -*- coding: utf-8 -*-
"""Docstring."""


# %% Imports
# %%% Py3 Standard
# from datetime import date
from random import randrange, random
from time import sleep
import urllib
from urllib.error import URLError
from typing import List

# %%% 3rd Party
from bs4 import BeautifulSoup
import pandas as pd
from dask.distributed import Client

# %%% User Defined
from nfetl._config import _config as config
from nfetl._datetime import _date


# %% Variables
# %%% System
__all__ = ['get_url_data', 'get_update']

# %%% Private
_client: object = Client(processes=False)

_default_start_year: int = int(config['Scope']['start_year'])
_default_urls: pd.DataFrame = pd.DataFrame([
    config['Scope']['keys'].split(', '),
    [config['URLs'][key] for key in config['URLs'].keys()],
    [config['Columns_extract'][key] for key in
     config['Columns_extract'].keys()]])


# %% Functions
def get_url_data(url: str) -> pd.DataFrame:
    """
    Retrieve data table from URL.

    Parameters
    ----------
    url : string
        URL for HTML page where data table is found.

    Returns
    -------
    data : pandas.DataFrame
        Data table from HTML page found at URL.
    """
    try:
        html: object = urllib.request.urlopen(url)
    except URLError:
        return None
    table: List[str] = BeautifulSoup(html, 'html.parser').findAll('tr')
    headers: List[str] = [i.getText() for i in table[1].findAll('th')]
    if len(headers) < 2:
        headers = [i.getText() for i in table[0].findAll('th')]
    done: bool = False
    while not done:
        # TODO: make this append rather than lumping all at once
        try:
            data = pd.concat(
                [pd.DataFrame([max([c.getText() for c in row],
                                   ['']*len(headers))],
                              columns=headers) for row in table])
            done = True
        except (ValueError, AttributeError) as e:
            if isinstance(e, AttributeError):
                i = 0
                for row in table:
                    for c in row:
                        if not hasattr(c, 'getText'):
                            i += 1
                        else:
                            break
                table = table[i:]
            else:
                i = 0
                for row in table:
                    if len(max([c.getText() for c in row],
                               ['']*len(headers))) == len(headers):
                        i += 1
                    else:
                        break
                table = table[:i]
    return data


def get_update(start_year: int = _default_start_year,
               urls: List[str] = _default_urls):
    """
    Retrieve all data tables from URL list provided by user or in config file.

    Makes multiple calls to get_url_data from the dask distributed client. Will
    retrieve data from start_year and after.

    Parameters
    ----------
    start_year : int, optional
        Four digit year for start of update. Default is from config file.
    urls : list, optional
        Contains URL strings from user input or config file. Default are from
        config file.

    Returns
    -------
    dfs : list
        Contains pandas.DataFrame objects containing data from table found on
        HTML page located at URL.
    """
    if start_year > _date.today().nfl_year():
        raise Exception
    dfs: List[pd.DataFrame] = []
    for url in urls:
        for y in range(start_year, _date.today().nfl_year() + 1):
            url = url.format(str(y))
            dfs.append(_client.submit(get_url_data, url=url))
            sleep(randrange(3, 6) + random())
    dfs = _client.gather(dfs)
    return dfs
