# -*- coding: utf-8 -*-
"""
nfetl is a package that can be used to create and maintain a database
containing nfl data by player/team by week/year.

@author: desch
"""


from configparser import ConfigParser
import os


self_path: str = os.path.dirname(__file__)
config_path: str = os.path.join(self_path, 'config.txt')

_config = ConfigParser()
_config.read(config_path)
_config['Data']['folder'] = os.path.join(self_path, 'data')
with open(config_path, 'w') as objFile:
    _config.write(objFile)
