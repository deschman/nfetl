# -*- coding: utf-8 -*-
"""core module of nfetl package."""


# %% Imports
# %%% Py3 Standard
import os
from configparser import ConfigParser
from argparse import ArgumentParser

# %%% 3rd Party
from dask import distributed


# %% Variables
_self_path: str = os.path.dirname(__file__)
_config_path: str = os.path.join(_self_path, 'config.txt')

_config = ConfigParser()
_config.read(_config_path)
_config['DEFAULT']['folder'] = os.path.join(os.path.dirname(_self_path), 'data')
with open(_config_path, 'w') as file:
    _config.write(file)

_argparser = ArgumentParser('nfetl')
_argparser.add_argument('directory',
                        default=os.path.join(os.path.dirname(os.sys.argv[0]),
                                             'NFL.db'),
                        help="full path to database")
_argparser.add_argument('--auto_update',
                        '-update',
                        nargs='?',
                        const=True,
                        default=False,
                        help="If present, database will not automatically " +
                             "update between start_year and end_year.")
_argparser.add_argument('--start_year',
                        '-start',
                        default=-1,
                        type=int,
                        help="Year that will be the start of update. The " +
                             "default is defined in the config file.")
_argparser.add_argument('--end_year',
                        '-end',
                        default=-1,
                        type=int,
                        help="Year that will be the end of update. The " +
                             "default is the current year.")


# %% Classes
class _DB:
    def __init__(self) -> None:
        raise NotImplementedError
