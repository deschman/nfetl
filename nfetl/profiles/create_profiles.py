# -*- coding: utf-8 -*-
"""
Created on Mon Jul 26 06:48:48 2021

@author: deschman
"""


if __name__ == '__main__':
    import os

    os.environ['MODIN_ENGINE'] = 'dask'

    # from modin import pandas as pd
    import pandas as pd
    from pandas_profiling import ProfileReport


    uri: str = r'sqlite:///C:\Users\deschman\spyder-env\Lib\site-packages\nfetl\nfetl\db\NFL.db'
    profile_dir: str = r'C:\Users\deschman\spyder-env\Lib\site-packages\NFetL\nfetl\profiles'
    """
    dim_date_df: pd.DataFrame = pd.read_sql("SELECT * FROM dim_date", uri)
    dim_date_report: ProfileReport = ProfileReport(dim_date_df, explorative=True, dark_mode=True)
    dim_date_report.to_file(os.path.join(profile_dir, 'dim_date Pandas Profile'))

    dim_team_df: pd.DataFrame = pd.read_sql("SELECT * FROM dim_team", uri)
    dim_date_report: ProfileReport = ProfileReport(dim_team_df, explorative=True, dark_mode=True)
    dim_date_report.to_file(os.path.join(profile_dir, 'dim_team Pandas Profile'))

    dim_franchise_df: pd.DataFrame = pd.read_sql("SELECT * FROM dim_franchise", uri)
    dim_date_report: ProfileReport = ProfileReport(dim_franchise_df, explorative=True, dark_mode=True)
    dim_date_report.to_file(os.path.join(profile_dir, 'dim_franchise Pandas Profile'))
    """
    dim_defense_df: pd.DataFrame = pd.read_sql("SELECT * FROM dim_defense", uri)
    dim_date_report: ProfileReport = ProfileReport(dim_defense_df, explorative=True, dark_mode=True)
    dim_date_report.to_file(os.path.join(profile_dir, 'dim_defense Pandas Profile'))
    """
    dim_coach_stint_df: pd.DataFrame = pd.read_sql("SELECT * FROM dim_coach_stint", uri)
    dim_date_report: ProfileReport = ProfileReport(dim_coach_stint_df, explorative=True, dark_mode=True)
    dim_date_report.to_file(os.path.join(profile_dir, 'dim_coach_stint Pandas Profile'))

    dim_coach_df: pd.DataFrame = pd.read_sql("SELECT * FROM dim_coach", uri)
    dim_date_report: ProfileReport = ProfileReport(dim_coach_df, explorative=True, dark_mode=True)
    dim_date_report.to_file(os.path.join(profile_dir, 'dim_coach Pandas Profile'))

    dim_player_stint_df: pd.DataFrame = pd.read_sql("SELECT * FROM dim_player_stint", uri)
    dim_date_report: ProfileReport = ProfileReport(dim_player_stint_df, explorative=True, dark_mode=True)
    dim_date_report.to_file(os.path.join(profile_dir, 'dim_player_stint Pandas Profile'))

    dim_player_df: pd.DataFrame = pd.read_sql("SELECT * FROM dim_player", uri)
    dim_date_report: ProfileReport = ProfileReport(dim_player_df, explorative=True, dark_mode=True)
    dim_date_report.to_file(os.path.join(profile_dir, 'dim_player Pandas Profile'))
    """
