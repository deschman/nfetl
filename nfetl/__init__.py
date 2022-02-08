# -*- coding: utf-8 -*-
"""Python package for creation and maintenance of an NFL statistics database."""


# %% Imports
# %%% Py3 Standard
import os

# %%% User Defined
from nfetl.db import DB


# %% Variables
# %%% Environment
os.envion['MODIN_ENGINE'] = 'dask'

# %%% System
__all__ = ['DB']
