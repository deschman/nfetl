# -*- coding: utf-8 -*-
"""Setup nfetl package."""


from setuptools import setup, find_packages


with open("README.md", 'r') as f:
    long_description: str = f.read()

setup(
      name='nfetl',
      version="0.1.0",
      description="A package for creating a database containing NFL stats.",
      long_description=long_description,
      author="Dan Eschman",
      author_email="deschman007@gmail.com",
      license="GNU AGPLv3",
      python_requires='~=3.8',
      install_requires=['pytest', 'bs4', 'pandas', 'dask'],
      packages=find_packages(),
      package_data={"test_URLData": ['data/test_URLData.h5']},
      long_description_content_type='text/markdown',
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Natural Language :: English',
          'Operating System :: Microsoft :: Windows :: Windows 10',
          'Programming Language :: Python :: 3',
          'Topic :: Database',
          'Topic :: Games/Entertainment',
          'Topic :: Internet',
          'Topic :: Text Processing'])
