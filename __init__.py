# -*- coding: utf-8 -*-
"""Python package for creation and maintenance of an NFL statistics database."""


# %% Imports
from nfetl.db import DB
from nfetl.processes import (get_url_data,
                             get_update)
from nfetl._datetime import _date as date


# %% Variables
__all__ = ['date',
           'DB',
           'get_url_data',
           'get_update',
           'land_data',
           'land_all_data']
