# -*- coding: utf-8 -*-


# %% Imports
# %%% Py3 Standard
from configparser import ConfigParser

# %%% 3rd Party
import pytest

# %%% User Defined
from nfetl._config import _config as config


# %% Functions
def test_config() -> None:
    assert isinstance(config, ConfigParser) and config.sections() == [
        'Scope', 'Data', 'URLs', 'Columns_extract', 'Columns_transform']
