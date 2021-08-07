name = 'nfl_data_py'

import pandas
import numpy
import datetime


def import_pbp_data(years, columns=None, downcast=True):
    """Imports play-by-play data
    
    Args:
        years (List[int]): years to get PBP data for
        columns (List[str]): only return these columns
        downcast (bool): convert float64 to float32, default True
    Returns:
        DataFrame
    """
    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')
        
    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')
    
    if columns is None:
        columns = []
        
    plays = pandas.DataFrame()

    url1 = r'https://github.com/nflverse/nflfastR-data/raw/master/data/play_by_play_'
    url2 = r'.parquet'

    for year in years:
        
        try:
            if len(columns) != 0:
                data = pandas.read_parquet(url1 + str(year) + url2, columns=columns, engine='fastparquet')
            else:
                data = pandas.read_parquet(url1 + str(year) + url2, engine='fastparquet')
            
            raw = pandas.DataFrame(data)
            raw['season'] = year

            if len(plays) == 0:
                plays = raw
            else:
                plays = plays.append(raw)
            
            print(str(year) + ' done.')
            
        except:
            print('Data not available for ' + str(year))
    
    # converts float64 to float32, saves ~30% memory
    if downcast:
        print('Downcasting floats.')
        cols = plays.select_dtypes(include=[numpy.float64]).columns
        plays.loc[:, cols] = plays.loc[:, cols].astype(numpy.float32)
            
    return plays


def import_weekly_data(years, columns=None, downcast=True):
    """Imports weekly player data
    
    Args:
        years (List[int]): years to get PBP data for
        columns (List[str]): only return these columns
        downcast (bool): convert float64 to float32, default True
    Returns:
        DataFrame
    """
    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')
        
    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')
    
    if columns is None:
        columns = []
        
    data = pandas.read_parquet(r'https://github.com/nflverse/nflfastR-data/raw/master/data/player_stats.parquet', engine='fastparquet')
    data = data[data['season'].isin(years)]

    if len(columns) > 0:
        data = data[columns]

    # converts float64 to float32, saves ~30% memory
    if downcast:
        print('Downcasting floats.')
        cols = data.select_dtypes(include=[numpy.float64]).columns
        data.loc[:, cols] = data.loc[:, cols].astype(numpy.float32)

    return data


def import_seasonal_data(years, s_type='REG'):
    
    if not isinstance(years, (list, range)):
        raise ValueError('years input must be list or range.')
        
    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')
        
    if s_type not in ('REG','ALL','POST'):
        raise ValueError('Only REG, ALL, POST allowed for s_type.')
    
    data = pandas.read_parquet(r'https://github.com/nflverse/nflfastR-data/raw/master/data/player_stats.parquet', engine='fastparquet')
    
    if s_type == 'ALL':
        data = data[data['season'].isin(years)]
        
    else:
        data = data[(data['season'].isin(years)) & (data['season_type'] == s_type)]
    
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
        ['player_id', 'player_name', 'season']).sum().reset_index()

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
    szn = data.groupby(['player_id', 'player_name', 'season', 'season_type']).sum().reset_index().merge(
        data[['player_id', 'season', 'season_type']].groupby(['player_id', 'season']).count().reset_index().rename(
            columns={'season_type': 'games'}), how='left', on=['player_id', 'season'])

    szn = szn.merge(season_stats[['player_id', 'season', 'tgt_sh', 'ay_sh', 'yac_sh', 'wopr', 'ry_sh', 'rtd_sh',
                                  'rfd_sh', 'rtdfd_sh', 'dom', 'w8dom', 'yptmpa', 'ppr_sh']], how='left',
                    on=['player_id', 'season'])

    return szn


def see_pbp_cols():

    data = pandas.read_parquet(r'https://github.com/nflverse/nflfastR-data/raw/master/data/play_by_play_2020.parquet', engine='fastparquet')
    cols = data.columns

    return cols


def see_weekly_cols():

    data = pandas.read_parquet(r'https://github.com/nflverse/nflfastR-data/raw/master/data/player_stats.parquet', engine='fastparquet')
    cols = data.columns

    return cols


def import_rosters(years, columns=None):

    if not isinstance(years, (list, range)):
        raise ValueError('years input must be list or range.')

    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')

    if columns is None:
        columns = []

    rosters = []

    for y in years:
        temp = pandas.read_csv(r'https://github.com/mrcaseb/nflfastR-roster/blob/master/data/seasons/roster_' + str(y)
                               + '.csv?raw=True', low_memory=False)
        rosters.append(temp)

    rosters = pandas.DataFrame(pandas.concat(rosters)).rename(
        columns={'full_name': 'player_name', 'gsis_id': 'player_id'})
    rosters.drop_duplicates(subset=['season', 'player_name', 'position', 'player_id'], keep='first', inplace=True)

    if len(columns) > 0:
        rosters = rosters[columns]

    def calc_age(x):
        ca = pandas.to_datetime(x[0])
        bd = pandas.to_datetime(x[1])
        return ca.year - bd.year + numpy.where(ca.month > bd.month, 0, -1)

    if 'birth_date' in columns and 'current_age' in columns:
    
        rosters['current_age'] = rosters['season'].apply(lambda x: datetime.datetime(int(x), 9, 1))
        rosters['age'] = rosters[['current_age', 'birth_date']].apply(calc_age, axis=1)
        rosters.drop(['current_age'], axis=1, inplace=True)
        rosters.dropna(subset=['player_id'], inplace=True)

    return rosters


def import_team_desc():
    
    df = pandas.read_csv(r'https://github.com/nflverse/nflfastR-data/raw/master/teams_colors_logos.csv')
    
    return df


def import_schedules(years):

    if not isinstance(years, (list, range)):
        raise ValueError('Input must be list or range.')
    
    if min(years) < 1999:
        raise ValueError('Data not available before 1999.')
    
    scheds = pandas.DataFrame()
            
    for x in years:
        
        try:
            temp = pandas.read_csv(r'https://raw.githubusercontent.com/cooperdff/nfl_data_py/main/data/schedules//' + str(x) + '.csv').drop('Unnamed: 0', axis=1)
            scheds = scheds.append(temp)
    
        except:
            print('Data not available for ' + str(x))
        
    return scheds
    

def import_win_totals(years):

    if not isinstance(years, (list, range)):
        raise ValueError('years variable must be list or range.')
    
    df = pandas.read_csv(r'https://raw.githubusercontent.com/nflverse/nfldata/master/data/win_totals.csv')
    
    df = df[df['season'].isin(years)]
    
    return df
    

def import_officials(years=None):

    if years is None:
        years = []
    
    if not isinstance(years, (list, range)):
        raise ValueError('years variable must be list or range.')

    df = pandas.read_csv(r'https://raw.githubusercontent.com/nflverse/nfldata/master/data/officials.csv')
    df['season'] = df['game_id'].str[0:4].astype(int)
    
    if len(years) > 0:
        df = df[df['season'].isin(years)]
    
    return df
    
    
def import_sc_lines(years=None):

    if years is None:
        years = []
    
    if not isinstance(years, (list, range)):
        raise ValueError('years variable must be list or range.')

    df = pandas.read_csv(r'https://raw.githubusercontent.com/nflverse/nfldata/master/data/sc_lines.csv')
    
    if len(years) > 0:
        df = df[df['season'].isin(years)]
    
    return df
    
    
def import_draft_picks(years=None):

    if years is None:
        years = []
    
    if not isinstance(years, (list, range)):
        raise ValueError('years variable must be list or range.')

    df = pandas.read_csv(r'https://raw.githubusercontent.com/nflverse/nfldata/master/data/draft_picks.csv')
    
    if len(years) > 0:
        df = df[df['season'].isin(years)]  
    
    return df
    

def import_draft_values(picks=None):

    if picks is None:
        picks = []
    
    if not isinstance(picks, (list, range)):
        raise ValueError('picks variable must be list or range.')

    df = pandas.read_csv(r'https://raw.githubusercontent.com/nflverse/nfldata/master/data/draft_values.csv')

    if len(picks) > 0:
        df = df[df['pick'].between(picks[0], picks[-1])]

    return df      


def import_combine_data(years=None, positions=None):
    
    if years is None:
        years = []
        
    if positions is None:
        positions = []
        
    if not isinstance(years, (list, range)):
        raise ValueError('years variable must be list or range.')
        
    if not isinstance(positions, list):
        raise ValueError('positions variable must be list.')
    
    df = pandas.read_csv(r'https://raw.githubusercontent.com/cooperdff/nfl_data_py/main/data/combine.csv')
    
    if len(years) > 0 and len(positions) > 0:
        df = df[(df['season'].isin(years)) & (df['position'].isin(positions))]
    elif len(years) > 0:
        df = df[df['season'].isin(years)]
    elif len(positions) > 0:
        df = df[df['position'].isin(positions)]

    return df    


def import_ids(columns=None, ids=None):
    
    avail_ids = ['mfl_id', 'sportradar_id', 'fantasypros_id', 'gsis_id', 'pff_id',
       'sleeper_id', 'nfl_id', 'espn_id', 'yahoo_id', 'fleaflicker_id',
       'cbs_id', 'rotowire_id', 'rotoworld_id', 'ktc_id', 'pfr_id',
       'cfbref_id', 'stats_id', 'stats_global_id', 'fantasy_data_id']
    avail_sites = [x[:-3] for x in avail_ids]
    
    if columns is None:
        columns = []
    
    if ids is None:
        ids = []

    if not isinstance(columns, list):
        raise ValueError('columns variable must be list.')
        
    if not isinstance(ids, list):
        raise ValueError('ids variable must be list.')
        
    if False in [x in avail_sites for x in ids]:
        raise ValueError('ids variable can only contain ' + ', '.join(avail_sites))
        
    df = pandas.read_csv(r'https://raw.githubusercontent.com/dynastyprocess/data/master/files/db_playerids.csv')
    
    rem_cols = [x for x in df.columns if x not in avail_ids]
    tgt_ids = [x + '_id' for x in ids]
        
    if len(columns) > 0 and len(ids) > 0:
        df = df[set(tgt_ids + columns)]
    elif len(columns) > 0 and len(ids) == 0:
        df = df[set(avail_ids + columns)]
    elif len(columns) == 0 and len(ids) > 0:
        df = df[set(tgt_ids + rem_cols)]
    
    return df
    

def clean_nfl_data(df):

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

    pro_tm_repl = {
        'GNB': 'GB',
        'KAN': 'KC',
        'LA': 'LAR',
        'LVR': 'LV',
        'NWE': 'NE',
        'NOR': 'NO',
        'SDG': 'SD',
        'SFO': 'SF',
        'TAM': 'TB'
    }

    if 'name' in df.columns:
        df.replace({'name': name_repl}, inplace=True)

    if 'col_team' in df.columns:
        df.replace({'col_team': col_tm_repl}, inplace=True)

        if 'name' in df.columns:
            for z in player_col_tm_repl:
                df[df['name'] == z[0]] = df[df['name'] == z[0]].replace({z[1]: z[2]})

    return df
