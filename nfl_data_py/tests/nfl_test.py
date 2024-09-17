from unittest import TestCase
from pathlib import Path
import shutil
import random

import pandas as pd

import nfl_data_py as nfl


class test_pbp(TestCase):
    pbp = nfl.import_pbp_data([2020])

    def test_is_df_with_data(self):
        self.assertIsInstance(self.pbp, pd.DataFrame)
        self.assertTrue(len(self.pbp) > 0)

    def test_is_df_with_data_thread_requests(self):
        s = nfl.import_pbp_data([2020, 2021], thread_requests=True)
        self.assertIsInstance(s, pd.DataFrame)
        self.assertTrue(len(s) > 0)
		
    def test_uses_cache_when_cache_is_true(self):
        cache = Path(__file__).parent/f"tmpcache-{random.randint(0, 10000)}"
        self.assertRaises(
            ValueError,
            nfl.import_pbp_data, [2020], cache=True, alt_path=cache
        )
        
        nfl.cache_pbp([2020], alt_path=cache)
        
        data = nfl.import_pbp_data([2020], cache=True, alt_path=cache)
        self.assertIsInstance(data, pd.DataFrame)
        
        shutil.rmtree(cache)

    def test_includes_participation_by_default(self):
        self.assertIn("offense_players", self.pbp.columns)

    def test_excludes_participation_when_requested(self):
        data = nfl.import_pbp_data([2020], include_participation=False)
        self.assertIsInstance(self.pbp, pd.DataFrame)
        self.assertTrue(len(self.pbp) > 0)
        self.assertNotIn("offense_players", data.columns)

    def test_excludes_participation_if_not_available(self):
        data = nfl.import_pbp_data([2024])
        self.assertIsInstance(self.pbp, pd.DataFrame)
        self.assertTrue(len(self.pbp) > 0)
        self.assertNotIn("offense_players", data.columns)
        
        
class test_weekly(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_weekly_data([2020])
        self.assertIsInstance(s, pd.DataFrame)
        self.assertTrue(len(s) > 0)

    def test_is_df_with_data_thread_requests(self):
        s = nfl.import_weekly_data([2020, 2021], thread_requests=True)
        self.assertIsInstance(s, pd.DataFrame)
        self.assertTrue(len(s) > 0)
        
class test_seasonal(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_seasonal_data([2020])
        self.assertIsInstance(s, pd.DataFrame)
        self.assertTrue(len(s) > 0)
        
class test_pbp_cols(TestCase):
    def test_is_list_with_data(self):
        s = nfl.see_pbp_cols()
        self.assertTrue(len(s) > 0)
        
class test_weekly_cols(TestCase):
    def test_is_list_with_data(self):
        s = nfl.see_weekly_cols()
        self.assertTrue(len(s) > 0)
        
class test_seasonal_rosters(TestCase):
    data = nfl.import_seasonal_rosters([2020])
    
    def test_is_df_with_data(self):
        self.assertIsInstance(self.data, pd.DataFrame)
        self.assertTrue(len(self.data) > 0)

    def test_import_multiple_years(self):
        s = nfl.import_weekly_rosters([2022, 2023])
        self.assertIsInstance(s, pd.DataFrame)
        self.assertGreater(len(s), len(self.data))
        self.assertListEqual(s.season.unique().tolist(), [2022, 2023])
        
    def test_computes_age_as_of_season_start(self):
        mahomes_ages = get_pat(self.data).age
        self.assertEqual(len(mahomes_ages), 1)
        self.assertEqual(mahomes_ages.iloc[0], 24)
        

class test_weekly_rosters(TestCase):
    data = nfl.import_weekly_rosters([2022])
        
    def test_is_df_with_data(self):
        assert isinstance(self.data, pd.DataFrame)
        self.assertGreater(len(self.data), 0)

    def test_import_multiple_years(self):
        s = nfl.import_weekly_rosters([2022, 2023])
        self.assertIsInstance(s, pd.DataFrame)
        self.assertGreater(len(s), len(self.data))
        self.assertListEqual(s.season.unique().tolist(), [2022, 2023])
        
    def test_gets_weekly_updates(self):
        assert isinstance(self.data, pd.DataFrame)
        hock = get_hock(self.data)
        self.assertCountEqual(hock[hock.team == 'DET'].week, [1, 2, 3, 4, 5, 7, 8])
        self.assertCountEqual(hock[hock.team == 'MIN'].week, range(9, 20))
        
    def test_computes_age_as_of_week(self):
        self.assertEqual(
            get_pat(self.data).sort_values("week").age.to_list(),
            [
                26.984, 26.995, 27.023, 27.042, 27.064, 27.08, 27.099,
                27.138, 27.157, 27.176, 27.195, 27.214, 27.233, 27.253,
                27.269, 27.291, 27.307, 27.346, 27.368, 27.406
            ]
        )
        
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
        
class test_win_totals(TestCase):
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

    def test_import_using_ids(self):
        ids = ["espn", "yahoo", "gsis"]
        s = nfl.import_ids(ids=ids)
        self.assertTrue(all([f"{id}_id" in s.columns for id in ids]))

    def test_import_using_columns(self):
        ret_columns = ["name", "birthdate", "college"]
        not_ret_columns = ["draft_year", "db_season", "team"]
        s = nfl.import_ids(columns=ret_columns)
        self.assertTrue(all([column in s.columns for column in ret_columns]))
        self.assertTrue(all([column not in s.columns for column in not_ret_columns]))

    def test_import_using_ids_and_columns(self):
        ret_ids = ["espn", "yahoo", "gsis"]
        ret_columns = ["name", "birthdate", "college"]
        not_ret_ids = ["cfbref_id", "pff_id", "prf_id"]
        not_ret_columns = ["draft_year", "db_season", "team"]
        s = nfl.import_ids(columns=ret_columns, ids=ret_ids)
        self.assertTrue(all([column in s.columns for column in ret_columns]))
        self.assertTrue(all([column not in s.columns for column in not_ret_columns]))
        self.assertTrue(all([f"{id}_id" in s.columns for id in ret_ids]))
        self.assertTrue(all([f"{id}_id" not in s.columns for id in not_ret_ids]))
        
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
        test_seasons = self.df.loc[self.df.season.isin(range(2018, 2023))]
        weeks_per_season = get_pat(test_seasons).groupby("season").week.nunique()
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
        
        
class test_ftn(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_ftn_data([2023])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)

    def test_is_df_with_data_thread_requests(self):
        s = nfl.import_ftn_data([2022, 2023], thread_requests=True)
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
		
        
class test_cache(TestCase):
    def test_cache(self):
        cache = Path(__file__).parent/f"tmpcache-{random.randint(0, 10000)}"
        self.assertFalse(cache.is_dir())
        
        nfl.cache_pbp([2020], alt_path=cache)
        
        self.assertTrue(cache.is_dir())

        pbp2020 = pd.read_parquet(cache/"season=2020"/"part.0.parquet")
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