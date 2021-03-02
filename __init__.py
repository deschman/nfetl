# -*- coding: utf-8 -*-
"""
A package for creation and maintenance of an nfl stats database.

@author: desch
"""


# %% Imports
# %%% Py3 Standard
from configparser import ConfigParser
import os

# %%% User Defined
from nfetl.db import *
from nfetl.processes import *


# %% Variables
# %%% System
__all__ = ['DB', 'get_url_data', 'get_update']

# %%% Public
self_path: str = os.path.dirname(__file__)

config_path: str = os.path.join(self_path, 'config.txt')

_config = ConfigParser()
_config.read(config_path)
_config['Data']['folder'] = os.path.join(self_path, 'data')
with open(config_path, 'w') as objFile:
    _config.write(objFile)
