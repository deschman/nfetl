# -*- coding: utf-8 -*-
"""Setup nfetl package."""


# %% Imports
from setuptools import setup, find_packages


# %% Script
with open("README.md", 'r') as f:
    long_description: str = f.read()

setup(name='nfetl',
      version="0.9.1",
      description="A package for creating a database containing NFL stats.",
      long_description=long_description,
      author="Dan Eschman",
      author_email="dan.eschman@gmail.com",
      license="GNU AGPLv3",
      python_requires='>=3.7',
      install_requires=['pytest', 'bs4', 'pandas', 'dask', 'modin'],
      packages=find_packages(),
      package_data={"test_url_data": ['data/test_url_data.h5']},
      long_description_content_type='text/markdown',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Natural Language :: English',
          'Operating System :: Microsoft :: Windows',
          'Programming Language :: Python :: 3',
          'Topic :: Database',
          'Topic :: Games/Entertainment',
          'Topic :: Internet',
          'Topic :: Text Processing'])
