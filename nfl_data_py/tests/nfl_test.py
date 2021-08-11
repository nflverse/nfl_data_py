from unittest import TestCase
import pandas as pd

import nfl_data_py as nfl

class test_pbp(TestCase):
    def test_is_df(self):
        s = nfl.import_pbp_data([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
		
class test_weekly(TestCase):
    def test_is_df(self):
        s = nfl.import_weekly_data([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
		
class test_seasonal(TestCase):
    def test_is_df(self):
        s = nfl.import_seasonal_data([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))
		
class test_pbp_cols(TestCase):
    def test_is_list(self):
        s = nfl.see_pbp_cols()
        self.assertEqual(True, isinstance(set(nfl.see_pbp_cols()), set))

class test_weekly_cols(TestCase):
    def test_is_list(self):
        s = nfl.see_weekly_cols()
        self.assertEqual(True, isinstance(set(nfl.see_pbp_cols()), set))

class test_rosters(TestCase):
    def test_is_df(self):
        s = nfl.import_rosters([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))

class test_team_desc(TestCase):
    def test_is_df(self):
        s = nfl.import_team_desc()
        self.assertEqual(True, isinstance(s, pd.DataFrame))

class test_schedules(TestCase):
    def test_is_df(self):
        s = nfl.import_schedules([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))

class test_wins(TestCase):
    def test_is_df(self):
        s = nfl.import_win_totals([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))

class test_officials(TestCase):
    def test_is_df(self):
        s = nfl.import_officials([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))

class test_draft_picks(TestCase):
    def test_is_df(self):
        s = nfl.import_draft_picks([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))		
		
class test_draft_values(TestCase):
    def test_is_df(self):
        s = nfl.import_draft_values()
        self.assertEqual(True, isinstance(s, pd.DataFrame))

class test_combine(TestCase):
    def test_is_df(self):
        s = nfl.import_combine_data([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))

class test_ids(TestCase):
    def test_is_df(self):
        s = nfl.import_ids()
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        
class test_ngs(TestCase):
    def test_is_df(self):
        s = nfl.import_ngs_data('passing')
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        
class test_clean(TestCase):
    def test_is_df(self):
        s = nfl.clean_nfl_data(nfl.import_weekly_data([2020]))
        self.assertEqual(True, isinstance(s, pd.DataFrame))

class test_depth_charts(TestCase):
    def test_is_df(self):
        s = nfl.import_depth_charts()
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        
class test_injuries(TestCase):
    def test_is_df(self):
        s = nfl.import_injuries()
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        
class test_qbr(TestCase):
    def test_is_df(self):
        s = nfl.import_qbr()
        self.assertEqual(True, isinstance(s, pd.DataFrame))
    
class test_pfr(TestCase):
    def test_is_df(self):
        s = nfl.import_pfr_passing()
        self.assertEqual(True, isinstance(s, pd.DataFrame))
        
class test_snaps(TestCase):
    def test_is_df(self):
        s = nfl.import_snap_counts([2020])
        self.assertEqual(True, isinstance(s, pd.DataFrame))