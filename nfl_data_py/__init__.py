name = 'nfl_data_py'

import os
import logging
import datetime
from warnings import warn
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy
import pandas
import appdirs
from urllib.error import HTTPError

# module level doc string
__doc__ = """
nfl_data_py - a Python package for working with NFL data
=========================================================

**nfl_data_py** is a Python package that streamlines the importing
of a variety of different American football datasets. It also includes
tables to assist with the merging of datasets from various sources.

Functions
---------
import_pbp_data() - import play-by-play data
import_weekly_data() - import weekly player stats
import_seasonal_data() - import seasonal player stats
import_snap_counts() - import weekly snap count stats
import_ngs_data() - import NGS advanced analytics
import_qbr() - import QBR for NFL or college
import_seasonal_pfr() - import advanced stats from PFR on a seasonal basis
import_weekly_pfr() - import advanced stats from PFR on a weekly basis
import_officials() - import details on game officials
import_schedules() - import weekly teams schedules
import_seasonal_rosters() - import yearly team rosters
import_weekly_rosters() - import team rosters by week, including in-season updates
import_players() - import descriptive data for all players
import_depth_charts() - import team depth charts
import_injuries() - import team injury reports
import_ids() - import mapping of player ids for more major sites
import_contracts() - import contract data
import_win_totals() - import win total lines for teams
import_sc_lines() - import weekly betting lines for teams
import_draft_picks() - import draft pick history
import_draft_values() - import draft value models by pick
import_combine_data() - import combine stats
import_ftn_data() - import FTN charting data
see_pbp_cols() - return list of play-by-play columns
see_weekly_cols() - return list of weekly stat columns
import_team_desc() - import descriptive data for team viz
cache_pbp() - save pbp files locally to allow for faster loading
clean_nfl_data() - clean df by aligning common name diffs
"""

def import_pbp_data(
        years, 
        columns=None, 
        include_participation=True, 
        downcast=True, 
        cache=False, 
        alt_path=None,
        thread_requests=False
    ):
    """Imports play-by-play data
    
    Args:
        years (List[int]): years to get PBP data for
        columns (List[str]): only return these columns
        include_participation (bool): whether to include participation stats or not
        downcast (bool): convert float64 to float32, default True
        cache (bool): whether to use local cache as source of pbp data
        alt_path (str): path for cache if not nfl_data_py default
    Returns:
        DataFrame
    """
    
    # check variable types
    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')
        
    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')
    
    if not columns:
        columns = []

    columns = [x for x in columns if x not in ['season']]
    
    if all([include_participation, len(columns) != 0]):
        columns = columns + [x for x in ['play_id','old_game_id'] if x not in columns]
       
    # potential sources for pbp data
    url1 = r'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_'
    url2 = r'.parquet'
    appname = 'nfl_data_py'
    appauthor = 'cooper_dff'
    pbp_data = []
    
    if cache:
        if not alt_path:
            dpath = os.path.join(appdirs.user_cache_dir(appname, appauthor), 'pbp')
        else:
            dpath = alt_path

    if thread_requests and not cache:
        with ThreadPoolExecutor() as executor:
            # Create a list of the same size as years, initialized with None
            pbp_data = [None]*len(years)
            # Create a mapping of futures to their corresponding index in the pbp_data
            futures_map = {
                executor.submit(
                    pandas.read_parquet,
                    path=url1 + str(year) + url2,
                    columns=columns if columns else None, 
                    engine='auto'
                ): idx 
                for idx, year in enumerate(years)
            }
            for future in as_completed(futures_map):
                pbp_data[futures_map[future]] = future.result()
    else:
        # read in pbp data
        for year in years:
            if cache:
                seasonStr = f'season={year}'
                if not os.path.isdir(os.path.join(dpath, seasonStr)):
                    raise ValueError(f'{year} cache file does not exist.')
                for fname in filter(lambda x: seasonStr in x, os.listdir(dpath)):
                    folder = os.path.join(dpath, fname)
                    for file in os.listdir(folder):
                        if file.endswith(".parquet"):
                            fpath = os.path.join(folder, file)
                
            # define path based on cache and alt_path variables
                path = fpath
            else:
                path = url1 + str(year) + url2

            # load data
            try:
                data = pandas.read_parquet(path, columns=columns if columns else None, engine='auto')

                raw = pandas.DataFrame(data)
                raw['season'] = year
                

                if include_participation and not cache:
                    path = r'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{}.parquet'.format(year)

                    try:
                        partic = pandas.read_parquet(path)
                        raw = raw.merge(
                            partic,
                            how='left',
                            left_on=['play_id','game_id'],
                            right_on=['play_id','nflverse_game_id']
                        )
                    except HTTPError:
                        pass
                
                pbp_data.append(raw)
                print(str(year) + ' done.')

            except Exception as e:
                print(e)
                print('Data not available for ' + str(year))
    
    if not pbp_data:
        return pandas.DataFrame()
    
    plays = pandas.concat(pbp_data, ignore_index=True)
    
    # converts float64 to float32, saves ~30% memory
    if downcast:
        print('Downcasting floats.')
        cols = plays.select_dtypes(include=[numpy.float64]).columns
        plays[cols] = plays[cols].astype(numpy.float32)
            
    return plays


def cache_pbp(years, downcast=True, alt_path=None):
    """Cache pbp data in local location to allow for faster loading

    Args:
        years (List[int]): years to cache PBP data for
        downcast (bool): convert float64 to float32, default True
        alt_path (str): path for cache if not nfl_data_py default
    Returns:
        DataFrame
    """

    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')

    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')

    url1 = r'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_'
    url2 = r'.parquet'
    appname = 'nfl_data_py'
    appauthor = 'nflverse'

    # define path for caching
    if alt_path is not None:
        path = str(alt_path)
    else:
        path = os.path.join(appdirs.user_cache_dir(appname, appauthor), 'pbp')

    # check if drectory exists already
    if not os.path.isdir(path):
        os.makedirs(path)

    # delete seasons to be replaced
    for folder in [os.path.join(path, x) for x in os.listdir(path) for y in years if ('season='+str(y)) in x]:
        for file in os.listdir(folder):
            if file.endswith(".parquet"):
                os.remove(os.path.join(folder, file))

    # read in pbp data
    for year in years:

        try:

            data = pandas.read_parquet(url1 + str(year) + url2, engine='auto')

            raw = pandas.DataFrame(data)
            raw['season'] = year

            if year >= 2016:
                path2 = r'https://github.com/nflverse/nflverse-data/releases/download/pbp_participation/pbp_participation_{}.parquet'.format(year)
                part = pandas.read_parquet(path2)
                raw = raw.merge(part, how='left', on=['play_id','old_game_id'])

            if downcast:
                cols = raw.select_dtypes(include=[numpy.float64]).columns
                raw[cols] = raw[cols].astype(numpy.float32)

            # write parquet to path, partitioned by season
            raw.to_parquet(path, partition_cols='season')

            print(str(year) + ' done.')

        except Exception as e:
            warn(
                f"Caching failed for {year}, skipping.\n"
                "In nfl_data_py 1.0, this will raise an exception.\n"
                f"Failure: {e}",
                DeprecationWarning,
                stacklevel=2
            )

            next
            

def import_weekly_data(
        years, 
        columns=None, 
        downcast=True,
        thread_requests=False
    ):
    """Imports weekly player data
    
    Args:
        years (List[int]): years to get weekly data for
        columns (List[str]): only return these columns
        downcast (bool): convert float64 to float32, default True
    Returns:
        DataFrame
    """
    
    # check variable types
    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')
        
    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')
    
    if not columns:
        columns = []

    url = r'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_{0}.parquet'

    if thread_requests:
        with ThreadPoolExecutor() as executor:
            # Create a list of the same size as years, initialized with None
            data = [None]*len(years)
            # Create a mapping of futures to their corresponding index in the data
            futures_map = {
                executor.submit(
                    pandas.read_parquet,
                    path=url.format(year),
                    columns=columns if columns else None,
                    engine='auto'
                ): idx
                for idx, year in enumerate(years)
            }
            for future in as_completed(futures_map):
                data[futures_map[future]] = future.result()
            data = pandas.concat(data)
    else:
        # read weekly data
        data = pandas.concat([pandas.read_parquet(url.format(x), engine='auto') for x in years])        

    if columns:
        data = data[columns]

    # converts float64 to float32, saves ~30% memory
    if downcast:
        print('Downcasting floats.')
        cols = data.select_dtypes(include=[numpy.float64]).columns
        data[cols] = data[cols].astype(numpy.float32)

    return data


def import_seasonal_data(years, s_type='REG'):
    """Imports seasonal player data
    
    Args:
        years (List[int]): years to get seasonal data for
        s_type (str): season type to include in average ('ALL','REG','POST')
    Returns:
        DataFrame
    """
    
    # check variable types
    if not isinstance(years, (list, range)):
        raise ValueError('years input must be list or range.')
        
    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')
        
    if s_type not in ('REG','ALL','POST'):
        raise ValueError('Only REG, ALL, POST allowed for s_type.')
    
    # import weekly data
    url = r'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_{0}.parquet'
    data = pandas.concat([pandas.read_parquet(url.format(x), engine='auto') for x in years])
    
    # filter to appropriate season_type
    if s_type != 'ALL':
        data = data[(data['season_type'] == s_type)]

    # calc per game stats
    pgstats = data[['recent_team', 'season', 'week', 'attempts', 'completions', 'passing_yards', 'passing_tds',
                      'passing_air_yards', 'passing_yards_after_catch', 'passing_first_downs',
                      'fantasy_points_ppr']].groupby(
        ['recent_team', 'season', 'week']).sum().reset_index()
    pgstats.columns = ['recent_team', 'season', 'week', 'atts', 'comps', 'p_yds', 'p_tds', 'p_ayds', 'p_yac', 'p_fds',
                       'ppr_pts']
    all_stats = data[
        ['player_id', 'player_name', 'recent_team', 'season', 'week', 'carries', 'rushing_yards', 'rushing_tds',
         'rushing_first_downs', 'rushing_2pt_conversions', 'receptions', 'targets', 'receiving_yards', 'receiving_tds',
         'receiving_air_yards', 'receiving_yards_after_catch', 'receiving_first_downs', 'receiving_epa',
         'fantasy_points_ppr']].merge(pgstats, how='left', on=['recent_team', 'season', 'week']).fillna(0)
    season_stats = all_stats.drop(['recent_team', 'week'], axis=1).groupby(
        ['player_id', 'season']).sum(numeric_only=True).reset_index()

    # calc custom receiving stats
    season_stats['tgt_sh'] = season_stats['targets'] / season_stats['atts']
    season_stats['ay_sh'] = season_stats['receiving_air_yards'] / season_stats['p_ayds']
    season_stats['yac_sh'] = season_stats['receiving_yards_after_catch'] / season_stats['p_yac']
    season_stats['wopr'] = season_stats['tgt_sh'] * 1.5 + season_stats['ay_sh'] * 0.8
    season_stats['ry_sh'] = season_stats['receiving_yards'] / season_stats['p_yds']
    season_stats['rtd_sh'] = season_stats['receiving_tds'] / season_stats['p_tds']
    season_stats['rfd_sh'] = season_stats['receiving_first_downs'] / season_stats['p_fds']
    season_stats['rtdfd_sh'] = (season_stats['receiving_tds'] + season_stats['receiving_first_downs']) / (
                season_stats['p_tds'] + season_stats['p_fds'])
    season_stats['dom'] = (season_stats['ry_sh'] + season_stats['rtd_sh']) / 2
    season_stats['w8dom'] = season_stats['ry_sh'] * 0.8 + season_stats['rtd_sh'] * 0.2
    season_stats['yptmpa'] = season_stats['receiving_yards'] / season_stats['atts']
    season_stats['ppr_sh'] = season_stats['fantasy_points_ppr'] / season_stats['ppr_pts']

    data.drop(['recent_team', 'week'], axis=1, inplace=True)
    szn = data.groupby(['player_id', 'season', 'season_type']).sum(numeric_only=True).reset_index().merge(
        data[['player_id', 'season', 'season_type']].groupby(['player_id', 'season']).count().reset_index().rename(
            columns={'season_type': 'games'}), how='left', on=['player_id', 'season'])

    szn = szn.merge(season_stats[['player_id', 'season', 'tgt_sh', 'ay_sh', 'yac_sh', 'wopr', 'ry_sh', 'rtd_sh',
                                  'rfd_sh', 'rtdfd_sh', 'dom', 'w8dom', 'yptmpa', 'ppr_sh']], how='left',
                    on=['player_id', 'season'])

    return szn


def see_pbp_cols():
    """Identifies list of columns in pbp data
    
    Returns:
        list
    """
    
    # load pbp file, identify columns
    data = pandas.read_parquet(r'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2020.parquet', engine='auto')
    cols = data.columns

    return cols


def see_weekly_cols():
    """Identifies list of columns in weekly data
    
    Returns:
        list
    """
    
    # load weekly file, identify columns
    data = pandas.read_parquet(r'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_2020.parquet', engine='auto')
    cols = data.columns

    return cols


def __import_rosters(release, years, columns=None):
    """Imports roster data
    
    Args:
        years (List[int]): years to get rosters for
        columns (List[str]): list of columns to return with DataFrame
        
    Returns:
        DataFrame
    """

    # check variable types
    if not isinstance(years, (list, range)):
        raise ValueError('years input must be list or range.')

    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')

    if release not in ('seasonal', 'weekly'):
        raise ValueError("release input must be 'seasonal' or 'weekly'.")
    
    if columns is not None and not (
        isinstance(columns, list) and
        all(isinstance(x, str) for x in columns) and
        len(columns) > 0
    ):
        raise ValueError('columns input must be a list of strings.')

    # Build the relevant URI for the release type
    uri = "https://github.com/nflverse/nflverse-data/releases/download/"
    if release == "seasonal":
        uri += "rosters/roster_{0}.parquet"
    elif release == "weekly":
        uri += "weekly_rosters/roster_weekly_{0}.parquet"

    # imports rosters for specified years
    rosters = pandas.concat([
        pandas.read_parquet(uri.format(y))
        for y in years
    ], ignore_index=True)
    
    # Post-import processing
    rosters['birth_date'] = pandas.to_datetime(rosters.birth_date)
    rosters.rename(
        columns={'gsis_id': 'player_id', 'full_name': 'player_name'},
        inplace=True
    )
    if columns:
        rosters = rosters[columns]

    return rosters


def import_weekly_rosters(years, columns=None):
    """Imports roster data including mid-season changes
    
    Args:
        years (List[int]): years to get rosters for
        columns (List[str]): list of columns to return with DataFrame
        
    Returns:
        DataFrame
    """
    rosters = __import_rosters("weekly", years, columns)
    
    scheds = pandas.read_csv("http://www.habitatring.com/games.csv")
    common_cols = ["season", "week", "gameday"]
    week_team_dates = pandas.concat([
        scheds[common_cols + ["home_team"]].rename(columns={"home_team": "team"}),
        scheds[common_cols + ["away_team"]].rename(columns={"away_team": "team"})
    ])
    roster_dates = pandas.to_datetime(
        rosters.merge(
            week_team_dates,
            on=["season", "week", "team"],
            how="left"
        ).gameday
    )
    rosters["age"] = ((roster_dates - rosters.birth_date).dt.days / 365.25).round(3)
    
    return rosters
    

def import_seasonal_rosters(years, columns=None):
    """Imports roster data as of the end of the season
    
    Args:
        years (List[int]): years to get rosters for
        columns (List[str]): list of columns to return with DataFrame
        
    Returns:
        DataFrame
    """
    
    rosters = __import_rosters("seasonal", years, columns)
    
    # calculate age in season
    if 'birth_date' in rosters.columns:
        rosters['szn_start'] = pandas.to_datetime(
            rosters.season.apply(lambda x: datetime.datetime(int(x), 9, 1))
        )
        rosters["age"] = (
            rosters.szn_start.dt.year - rosters.birth_date.dt.year  + numpy.where(
                rosters.szn_start.dt.month > rosters.birth_date.dt.month, 0, -1
            )
        )
        rosters.drop(['szn_start'], axis=1, inplace=True)
        
    rosters.dropna(subset=['player_id'], inplace=True)

    return rosters


def import_players():
    """Import descriptive data for all players
    
    Returns:
        DataFrame
    """
    df = pandas.read_parquet(r'https://github.com/nflverse/nflverse-data/releases/download/players/players.parquet')
    return df
    
    
def import_team_desc():
    """Import team descriptive data
    
    Returns:
        DataFrame
    """
    
    # import desc data
    df = pandas.read_csv(r'https://github.com/nflverse/nflfastR-data/raw/master/teams_colors_logos.csv')
    
    return df


def import_schedules(years):
    """Import schedules
    
    Args:
        years (List[int]): years to get schedules for
        
    Returns:
        DataFrame
    """
    
    # check variable types
    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')
    
    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')
    
    scheds = pandas.DataFrame()
    
    # import schedule for specified years
    scheds = pandas.read_csv(r'http://www.habitatring.com/games.csv')    
    scheds = scheds[scheds['season'].isin(years)]
        
    return scheds
    

def import_win_totals(years = None):
    """Import win total projections
    
    Args:
        years (List[int]): years to get win totals for
        
    Returns:
        DataFrame
    """
    
    logging.warning(
        "The win totals data source is currently in flux and may be out of date."
    )

    # check variable types
    if not isinstance(years, (list, range, type(None))):
        raise ValueError('years variable must be list or range.')
    
    # import win totals
    url = "https://raw.githubusercontent.com/mrcaseb/nfl-data/master/data/nfl_lines_odds.csv.gz"
    df = pandas.read_csv(url).loc[lambda df: df.game_id.notna()]
    df["season"] = df.game_id.str[:4].astype(int)

    return df[df['season'].isin(years)] if years else df
    

def import_officials(years=None):
    """Import game officials
    
    Args:
        years (List[int]): years to get officials for
        
    Returns:
        DataFrame
    """

    # check variable types
    if years is None:
        years = []
    
    if not isinstance(years, (list, range)):
        raise ValueError('years variable must be list or range.')

    # import officials data
    df = pandas.read_csv(r'https://raw.githubusercontent.com/nflverse/nfldata/master/data/officials.csv')
    df['season'] = df['game_id'].str[0:4].astype(int)
    
    if len(years) > 0:
        df = df[df['season'].isin(years)]
    
    return df
    
    
def import_sc_lines(years=None):
    """Import weekly scoring lines
    
    Args:
        years (List[int]): years to get scoring lines for
       
    Returns:
        DataFrame
    """

    # check variable types
    if years is None:
        years = []
    
    if not isinstance(years, (list, range)):
        raise ValueError('years variable must be list or range.')
    
    # import data
    df = pandas.read_csv(r'https://raw.githubusercontent.com/nflverse/nfldata/master/data/sc_lines.csv')
    
    if len(years) > 0:
        df = df[df['season'].isin(years)]
    
    return df
    
    
def import_draft_picks(years=None):
    """Import draft picks
    
    Args:
        years (List[int]): years to get draft picks for
    
    Returns:
        DataFrame
    """

    # check variable types
    if years is None:
        years = []
    
    if not isinstance(years, (list, range)):
        raise ValueError('years variable must be list or range.')

    # import draft pick data
    df = pandas.read_parquet(r'https://github.com/nflverse/nflverse-data/releases/download/draft_picks/draft_picks.parquet', engine='auto')
    
    if len(years) > 0:
        df = df[df['season'].isin(years)]  
    
    return df
    

def import_draft_values(picks=None):
    """Import draft pick values from variety of models
    
    Args:
        picks (List[int]): subset of picks to return values for
        
    Returns:
        DataFrame
    """

    # check variable types
    if picks is None:
        picks = []
    
    if not isinstance(picks, (list, range)):
        raise ValueError('picks variable must be list or range.')

    # import data
    df = pandas.read_csv(r'https://raw.githubusercontent.com/nflverse/nfldata/master/data/draft_values.csv')

    if len(picks) > 0:
        df = df[df['pick'].between(picks[0], picks[-1])]

    return df      


def import_combine_data(years=None, positions=None):
    """Import combine results for all position groups
    
    Args:
        years (List[str]): years to get combine data for
        positions (List[str]): list of positions to get data for
        
    Returns:
        DataFrame
    """
    
    # check variable types
    if years is None:
        years = []
        
    if positions is None:
        positions = []
        
    if not isinstance(years, (list, range)):
        raise ValueError('years variable must be list or range.')
        
    if not isinstance(positions, list):
        raise ValueError('positions variable must be list.')
        
    # import data
    df = pandas.read_parquet(r'https://github.com/nflverse/nflverse-data/releases/download/combine/combine.parquet', engine='auto')
    
    # filter to years and positions
    if len(years) > 0 and len(positions) > 0:
        df = df[(df['season'].isin(years)) & (df['pos'].isin(positions))]
    elif len(years) > 0:
        df = df[df['season'].isin(years)]
    elif len(positions) > 0:
        df = df[df['pos'].isin(positions)]

    return df    


def import_ids(columns=None, ids=None):
    """Import mapping table of ids for most major data providers
    
    Args:
        columns (Iterable[str]): list of columns to return
        ids (Iterable[str]): list of specific ids to return
        
    Returns:
        DataFrame
    """

    columns = columns or []
    if not isinstance(columns, Iterable):
        raise ValueError('columns argument must be a list.')

    ids = ids or []
    if not isinstance(ids, Iterable):
        raise ValueError('ids argument must be a list.')
        
    df = pandas.read_csv("https://raw.githubusercontent.com/dynastyprocess/data/master/files/db_playerids.csv")
    
    id_cols = [c for c in df.columns if c.endswith('_id')]
    non_id_cols = [c for c in df.columns if not c.endswith('_id')]
    
    # filter df to just specified ids + columns
    ret_ids = [x + '_id' for x in ids] or id_cols
    ret_cols = columns or non_id_cols
    ret_columns = list(set([*ret_ids, *ret_cols]))

    return df[ret_columns]
    

def import_contracts():
    """Imports historical contract data
    
    Returns:
        DataFrame
    """
    
    df = pandas.read_parquet(r'https://github.com/nflverse/nflverse-data/releases/download/contracts/historical_contracts.parquet')
    
    return df
    
    
def import_ngs_data(stat_type, years=None):
    """Imports seasonal NGS data
    
    Args:
        stat_type (str): type of stats to pull (receiving, passing, rushing)
        years (List[int]): years to get PBP data for, optional
    Returns:
        DataFrame
    """
    
    # check variable types
    if years is None:
        years = []
        
    if stat_type not in ('receiving','passing','rushing'):
        raise ValueError('stat_type must be one of receiving, passing, rushing.')
        
    if not isinstance(years, (list, range)):
        raise ValueError('years variable must be list or range.')
    
    # import data
    url = r'https://github.com/nflverse/nflverse-data/releases/download/nextgen_stats/ngs_{0}.parquet'.format(stat_type)
    data = pandas.read_parquet(url)
    
    if len(years) > 0:
        data = data[data['season'].isin([x for x in years])]
    
    # return
    return data
    

def import_depth_charts(years):
    """Imports team depth charts
    
    Args:
        years (List[int]): years to return depth charts for, optional
    Returns:
        DataFrame
    """

    # check variable types
    if years is None:
        raise ValueError('Must specify timeframe.')
        
    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')
    
    if len(years) > 0:
        if min(years) < 2001:
            raise ValueError('Data not available before 2001.')
    
    # import data
    url = r'https://github.com/nflverse/nflverse-data/releases/download/depth_charts/depth_charts_{0}.parquet'

    df = pandas.concat([pandas.read_parquet(url.format(x), engine='auto') for x in years])
    
    return df
    

def import_injuries(years):
    """Imports team injury reports
    
    Args:
        years (List[int]): years to return injury reports for, optional
    Returns:
        DataFrame
    """

    # check variable types
    if years is None:
        raise ValueError('Must specify timeframe.')
        
    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')
    
    if len(years) > 0:
        if min(years) < 2009:
            raise ValueError('Data not available before 2009.')
    
    #import data
    url = r'https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_{0}.parquet'

    df = pandas.concat([pandas.read_parquet(url.format(x), engine='auto') for x in years])
    
    return df
    

def import_qbr(years=None, level='nfl', frequency='season'):
    """Import NFL or college QBR data
    
    Args:
        years (List[int]): list of years to return data for, optional
        level (str): level to pull data, nfl or college, default to nfl
        frequency (str): frequency to pull data, weekly or season, default to season
    Returns:
        DataFrame
    """

    # check variable types and specifics
    if years is None:
        years = []
        
    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')
    
    if len(years) > 0:
        if min(years) < 2006:
            raise ValueError('Data not available before 2006.')
    
    if level not in ('nfl','college'):
        raise ValueError('level must be nfl or college')
        
    if frequency not in ('season','weekly'):
        raise ValueError('frequency must be season or weekly')
    
    # import data
    url = r'https://raw.githubusercontent.com/nflverse/espnscrapeR-data/master/data/qbr-{}-{}.csv'.format(level, frequency)

    df = pandas.read_csv(url)
            
    # filter to desired years
    if len(years) > 0:
        df = df[df['season'].between(min(years), max(years))]
    
    return df


def __validate_pfr_inputs(s_type, years=None):
    if s_type not in ('pass', 'rec', 'rush', 'def'):
        raise ValueError('s_type variable must be one of "pass", "rec","rush", or "def".')
    
    if years is None:
        return []
    
    if not isinstance(years, Iterable):
        raise ValueError("years must be an Iterable.")
    
    years = list(years)

    if not all(isinstance(x, int) for x in years):
        raise ValueError('years variable must only contain integers.')

    if years and min(years) < 2018:
        raise ValueError('Data not available before 2018.')

    return years
    
def import_seasonal_pfr(s_type, years=None):
    """Import PFR advanced season-level statistics
    
    Args:
        s_type (str): must be one of pass, rec, rush, def
        years (List[int]): years to return data for, optional
    Returns:
        DataFrame
    """
    
    years = __validate_pfr_inputs(s_type, years)

    url = f"https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_season_{s_type}.parquet"
    df = pandas.read_parquet(url)

    return df[df.season.isin(years)] if years else df


def import_weekly_pfr(s_type, years=None):
    """Import PFR advanced week-level statistics
    
    Args:
        s_type (str): must be one of pass, rec, rush, def
        years (List[int]): years to return data for, optional
    Returns:
        DataFrame
    """

    years = __validate_pfr_inputs(s_type, years)
    
    if len(years) == 0:
        years = list(import_seasonal_pfr(s_type).season.unique())
    
    url = "https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_week_{0}_{1}.parquet"
    df = pandas.concat([
        pandas.read_parquet(url.format(s_type, yr))
        for yr in years
    ])
    
    return df[df.season.isin(years)] if years else df
    
    
def import_snap_counts(years):
    """Import snap count data for individual players
    
    Args:
        years (List[int]): years to return snap counts for
    Returns:
        DataFrame
    """

    # check variables types
    if years is None:
        raise ValueError('Must provide years variable.')
        
    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')
    
    if len(years) > 0:
        if min(years) < 2012:
            raise ValueError('Data not available before 2012.')
    
    df = pandas.DataFrame()
    
    # import data
        
    url = r'https://github.com/nflverse/nflverse-data/releases/download/snap_counts/snap_counts_{0}.parquet'

    df = pandas.concat([pandas.read_parquet(url.format(x)) for x in years])
    
    return df



def import_ftn_data(
        years, 
        columns=None, 
        downcast=True,
        thread_requests=False
    ):
    """Imports FTN charting data
    
    FTN Data manually charts plays and has graciously provided a subset of their
    charting data to be published via the nflverse. Data is available from the 2022
    season onwards and is charted within 48 hours following each game. This data
    is released under the [CC-BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/)
    Creative Commons license and attribution must be made to **FTN Data via nflverse**
    
    Args:
        years (List[int]): years to get weekly data for
        columns (List[str]): only return these columns, default None
        downcast (bool): convert float64 to float32, default True
        thread_requests (bool): use thread pool to read files, default False
    Returns:
        DataFrame
    """
    
    # check variable types
    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')
        
    if min(years) < 2022:
        raise ValueError('Data not available before 2022.')

    url = r'https://github.com/nflverse/nflverse-data/releases/download/ftn_charting/ftn_charting_{0}.parquet'

    if thread_requests:
        with ThreadPoolExecutor() as executor:
            # Create a list of the same size as years, initialized with None
            data = [None]*len(years)
            # Create a mapping of futures to their corresponding index in the data
            futures_map = {
                executor.submit(
                    pandas.read_parquet,
                    path=url.format(year),
                    columns=columns if columns else None,
                    engine='auto'
                ): idx
                for idx, year in enumerate(years)
            }
            for future in as_completed(futures_map):
                data[futures_map[future]] = future.result()
            data = pandas.concat(data)
    else:
        # read charting data
        data = pandas.concat([pandas.read_parquet(url.format(x), engine='auto', columns=columns) for x in years])

    # converts float64 to float32, saves ~30% memory
    if downcast:
        print('Downcasting floats.')
        cols = data.select_dtypes(include=[numpy.float64]).columns
        data[cols] = data[cols].astype(numpy.float32)

    return data

    
def clean_nfl_data(df):
    """Cleans descriptive data for players and teams to help with consistency across datasets
    
    Args:
        df (DataFrame): DataFrame to be cleaned
        
    Returns:
        DataFrame
    """

    name_repl = {
        'Gary Jennings Jr': 'Gary Jennings',
        'DJ Chark': 'D.J. Chark',
        'Cedrick Wilson Jr.': 'Cedrick Wilson',
        'Deangelo Yancey': 'DeAngelo Yancey',
        'Ardarius Stewart': 'ArDarius Stewart',
        'Calvin Johnson  HOF': 'Calvin Johnson',
        'Mike Sims-Walker': 'Mike Walker',
        'Kenneth Moore': 'Kenny Moore',
        'Devante Parker': 'DeVante Parker',
        'Brandon Lafell': 'Brandon LaFell',
        'Desean Jackson': 'DeSean Jackson',
        'Deandre Hopkins': 'DeAndre Hopkins',
        'Deandre Smelter': 'DeAndre Smelter',
        'William Fuller': 'Will Fuller',
        'Lavon Brazill': 'LaVon Brazill',
        'Devier Posey': 'DeVier Posey',
        'Demarco Sampson': 'DeMarco Sampson',
        'Deandrew Rubin': 'DeAndrew Rubin',
        'Latarence Dunbar': 'LaTarence Dunbar',
        'Jajuan Dawson': 'JaJuan Dawson',
        "Andre' Davis": 'Andre Davis',
        'Johnathan Holland': 'Jonathan Holland',
        'Johnnie Lee Higgins Jr.': 'Johnnie Lee Higgins',
        'Marquis Walker': 'Marquise Walker',
        'William Franklin': 'Will Franklin',
        'Ted Ginn Jr.': 'Ted Ginn',
        'Jonathan Baldwin': 'Jon Baldwin',
        'T.J. Graham': 'Trevor Graham',
        'Odell Beckham Jr.': 'Odell Beckham',
        'Michael Pittman Jr.': 'Michael Pittman',
        'DK Metcalf': 'D.K. Metcalf',
        'JJ Arcega-Whiteside': 'J.J. Arcega-Whiteside',
        'Lynn Bowden Jr.': 'Lynn Bowden',
        'Laviska Shenault Jr.': 'Laviska Shenault',
        'Henry Ruggs III': 'Henry Ruggs',
        'KJ Hamler': 'K.J. Hamler',
        'KJ Osborn': 'K.J. Osborn',
        'Devonta Smith': 'DeVonta Smith',
        'Terrace Marshall Jr.': 'Terrace Marshall',
        "Ja'Marr Chase": 'JaMarr Chase'
    }

    col_tm_repl = {
        'Ole Miss': 'Mississippi',
        'Texas Christian': 'TCU',
        'Central Florida': 'UCF',
        'Bowling Green State': 'Bowling Green',
        'West. Michigan': 'Western Michigan',
        'Pitt': 'Pittsburgh',
        'Brigham Young': 'BYU',
        'Texas-El Paso': 'UTEP',
        'East. Michigan': 'Eastern Michigan',
        'Middle Tenn. State': 'Middle Tennessee State',
        'Southern Miss': 'Southern Mississippi',
        'Louisiana State': 'LSU'
    }

    na_replace = {
        'NA':numpy.nan
    }

    for col in df.columns:
        if df[col].dtype == 'object':
            df.replace({col:na_replace}, inplace=True)

    if 'name' in df.columns:
        df.replace({'name': name_repl}, inplace=True)

    if 'col_team' in df.columns:
        df.replace({'col_team': col_tm_repl}, inplace=True)

    return df
