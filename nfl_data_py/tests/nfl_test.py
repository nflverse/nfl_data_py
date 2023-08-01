from unittest import TestCase
import pandas as pd

import nfl_data_py as nfl

class test_pbp(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_pbp_data([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
		
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
        
class test_seasonal_rosters(TestCase):
    data = nfl.import_seasonal_rosters([2020])
    
    def test_is_df_with_data(self):
        self.assertEqual(True, isinstance(self.data, pd.DataFrame))
        self.assertTrue(len(self.data) > 0)
        
    def test_computes_age_as_of_season_start(self):
        mahomes_ages = self.data[get_pat].age
        self.assertEqual(len(mahomes_ages), 1)
        self.assertEqual(mahomes_ages.iloc[0], 24)
        

class test_weekly_rosters(TestCase):
    data = nfl.import_weekly_rosters([2022])
        
    def test_is_df_with_data(self):
        assert isinstance(self.data, pd.DataFrame)
        self.assertGreater(len(self.data), 0)
        
    def test_gets_weekly_updates(self):
        assert isinstance(self.data, pd.DataFrame)
        hock = self.data[get_hock]
        self.assertCountEqual(hock[hock.team == 'DET'].week, [1, 2, 3, 4, 5, 7, 8])
        self.assertCountEqual(hock[hock.team == 'MIN'].week, range(9, 20))
        
    def test_computes_age_as_of_week(self):
        self.assertEqual(
            self.data[get_pat].age.to_list(),
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
        
class test_wins(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_win_totals([2020])
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
    def test_is_df_with_data(self):
        s = nfl.import_combine_data([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
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
        
class test_pfr(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_pfr('pass')
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_snaps(TestCase):
    def test_is_df_with_data(self):
        s = nfl.import_snap_counts([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        self.assertTrue(len(s) > 0)
        
class test_cache(TestCase):
    def test_cache(self):
        nfl.cache_pbp([2020])
        s = nfl.import_pbp_data([2020], cache=True)
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        
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
def get_pat(row):
    return row.player_name == 'Patrick Mahomes'

def get_hock(row):
    return row.player_name == 'T.J. Hockenson'