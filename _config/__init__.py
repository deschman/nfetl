# -*- coding: utf-8 -*-
"""Load configured variables."""


# %% Imports
from configparser import ConfigParser
import os


# %% Variables
_self_path: str = os.path.dirname(__file__)
_config_path: str = os.path.join(_self_path, 'config.txt')

_config = ConfigParser()
_config.read(_config_path)
_config['Data']['folder'] = os.path.join(os.path.dirname(_self_path), 'data')
with open(_config_path, 'w') as file:
    _config.write(file)
