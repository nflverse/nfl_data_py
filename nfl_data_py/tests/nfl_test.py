from unittest import TestCase
from pathlib import Path
import shutil

import pandas as pd

import nfl_data_py as nfl


class test_pbp(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_pbp_data([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
    def test_uses_cache_when_cache_is_true(self):
        cache = Path(__file__).parent/"tmpcache"
        self.assertRaises(
            ValueError,
            nfl.import_pbp_data, [2020], cache=True, alt_path=cache
        )
        
        nfl.cache_pbp([2020], alt_path=cache)
        
        data = nfl.import_pbp_data([2020], cache=True, alt_path=cache)
        self.assertIsInstance(data, pd.DataFrame)
        
        shutil.rmtree(cache)
        
class test_weekly(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_weekly_data([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_seasonal(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_seasonal_data([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_pbp_cols(TestCase):
    def test_is_list_with_data(self):
        s = nfl.see_pbp_cols()
        self.assertEqual(True, isinstance(set(nfl.see_pbp_cols()), set))
        self.assertTrue(len(s) > 0)
        
class test_weekly_cols(TestCase):
    def test_is_list_with_data(self):
        s = nfl.see_weekly_cols()
        self.assertEqual(True, isinstance(set(nfl.see_pbp_cols()), set))
        self.assertTrue(len(s) > 0)
        
class test_rosters(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_rosters([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_team_desc(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_team_desc()
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_schedules(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_schedules([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_wins(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_win_totals([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
    def test_is_df_with_data_no_years(self):
        s = nfl.import_win_totals()
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_officials(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_officials([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_draft_picks(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_draft_picks([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))		
        self.assertTrue(len(s) > 0)
        
class test_draft_values(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_draft_values()
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_combine(TestCase):
    def test_is_df_with_data_no_years_no_positions(self):
        s = nfl.import_combine_data()
        self.assertIsInstance(s, pd.DataFrame)
        self.assertFalse(s.empty)
        
    def test_is_df_with_data_with_years_no_positions(self):
        s = nfl.import_combine_data([2020])
        self.assertIsInstance(s, pd.DataFrame)
        self.assertFalse(s.empty)
        
    def test_is_df_with_data_no_years_with_positions(self):
        s = nfl.import_combine_data(positions=["QB"])
        self.assertIsInstance(s, pd.DataFrame)
        self.assertFalse(s.empty)
        
    def test_is_df_with_data_with_years_and_positions(self):
        s = nfl.import_combine_data([2020], positions=["QB"])
        self.assertIsInstance(s, pd.DataFrame)
        self.assertFalse(s.empty)
        
class test_ids(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_ids()
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_ngs(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_ngs_data('passing')
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_clean(TestCase):
    def test_is_df_with_data(self):
        s = nfl.clean_nfl_data(nfl.import_weekly_data([2020]))
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_depth_charts(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_depth_charts([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_injuries(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_injuries([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_qbr(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_qbr()
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_seasonal_pfr(TestCase):
    df = nfl.import_seasonal_pfr('pass')
    
    def test_is_df_with_data(self):
        self.assertIsInstance(self.df, pd.DataFrame)
        self.assertTrue(len(self.df) > 0)
        
    def test_contains_one_row_per_player_per_season(self):
        pat = get_pat(self.df)
        self.assertCountEqual(pat.season, pat.season.unique())
        
    def test_contains_seasonal_exclusive_columns(self):
        self.assertIn("rpo_plays", self.df.columns)
    
    def test_retrieves_all_available_years_by_default(self):
        available_years = pd.read_parquet(
            "https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_season_pass.parquet"
        ).season.unique()
        self.assertCountEqual(self.df.season.unique(), available_years)
        
    def test_filters_by_year(self):
        only_20_21 = nfl.import_seasonal_pfr('pass', [2020, 2021])
        self.assertCountEqual(only_20_21.season.unique(), [2020, 2021])
        
class test_weekly_pfr(TestCase):
    df = nfl.import_weekly_pfr('pass')
    
    def test_is_df_with_data(self):
        self.assertIsInstance(self.df, pd.DataFrame)
        self.assertTrue(len(self.df) > 0)
        
    def test_contains_one_row_per_player_per_week(self):
        weeks_per_season = get_pat(self.df).groupby("season").week.nunique()
        self.assertEqual(weeks_per_season.to_list(), [18, 17, 18, 20, 20])
        
    def test_does_not_contain_seasonal_exclusive_columns(self):
        self.assertNotIn("rpo_plays", self.df.columns)
    
    def test_retrieves_all_available_years_by_default(self):
        available_years = pd.read_parquet(
            "https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/advstats_season_pass.parquet"
        ).season.unique()
        self.assertCountEqual(self.df.season.unique(), available_years)
        
    def test_filters_by_year(self):
        only_20_21 = nfl.import_weekly_pfr('pass', [2020, 2021])
        self.assertCountEqual(only_20_21.season.unique(), [2020, 2021])
    
        
class test_snaps(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_snap_counts([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_cache(TestCase):
    def test_cache(self):
        cache = Path(__file__).parent/"tmpcache"
        self.assertFalse(cache.is_dir())
        
        nfl.cache_pbp([2020], alt_path=cache)
        
        new_paths = list(cache.glob("**/*"))
        self.assertEqual(len(new_paths), 2)
        self.assertTrue(new_paths[0].is_dir())
        self.assertTrue(new_paths[1].is_file())
        
        pbp2020 = pd.read_parquet(new_paths[1])
        self.assertIsInstance(pbp2020, pd.DataFrame)
        self.assertFalse(pbp2020.empty)
        
        shutil.rmtree(cache)
        
class test_contracts(TestCase):
    def test_contracts(self):
        s = nfl.import_contracts()
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_players(TestCase):
    def test_players(self):
        s = nfl.import_players()
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)


# ---------------------------- Helper Functions -------------------------------
def __get_player(df: pd.DataFrame, player_name: str):
    player_name_cols = ('player_name', 'player', 'pfr_player_name')
    player_name_col = set(player_name_cols).intersection(set(df.columns)).pop()
    return df[df[player_name_col] == player_name]

def get_pat(df):
    return __get_player(df, 'Patrick Mahomes')

def get_hock(df):
    return __get_player(df, 'T.J. Hockenson')