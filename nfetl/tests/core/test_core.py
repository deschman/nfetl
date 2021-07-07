# -*- coding: utf-8 -*-
"""Test core functions of nfetl package."""


# %% Imports
# %%% Py3 Standard
from configparser import ConfigParser

# %%% 3rd Party
import pytest

# %%% User Defined
from nfetl.core import _config, _DB


# %% Functions
def test_config() -> None:
    assert isinstance(_config, ConfigParser) and _config.sections() == [
        'offense',
        'kicking',
        'op_defense',
        'rb_defense',
        'te_defense',
        'qb_defense',
        'wr_defense',
        'coaches',
        'schedule',
        'test']


def test__DB() -> None:
    with pytest.raises(NotImplementedError):
        _DB()
