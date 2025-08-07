from unittest import TestCase
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import nflplotpy as nflplot
from nflplotpy.core.plotting import plot_team_stats
from nflplotpy.core.colors import get_team_colors, NFLColorPalette
from nflplotpy.core.utils import validate_teams, team_factor, team_tiers


class test_comprehensive_plotting(TestCase):
    """Comprehensive tests for nflplotpy plotting functionality."""
    
    def setUp(self):
        """Set up test data."""
        self.sample_team_data = pd.DataFrame({
            'team': ['KC', 'GB', 'NE', 'DAL', 'SF', 'BUF'],
            'offensive_epa': [0.15, 0.08, 0.05, -0.02, 0.12, 0.07],
            'defensive_epa': [-0.08, -0.05, -0.12, 0.03, -0.09, -0.04],
            'points_per_game': [28.5, 24.2, 21.8, 19.4, 26.1, 23.7],
            'points_allowed': [18.2, 22.1, 16.8, 25.3, 19.6, 21.4]
        })
    
    def test_basic_team_stats_plot(self):
        """Test basic team stats plotting functionality."""
        fig = plot_team_stats(
            self.sample_team_data,
            x='offensive_epa',
            y='defensive_epa',
            backend='matplotlib',
            show_logos=False,  # Disable logos for basic test
            title='Basic Team Stats Test'
        )
        
        self.assertIsInstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_team_stats_plot_with_reference_lines(self):
        """Test plotting with reference lines."""
        fig = plot_team_stats(
            self.sample_team_data,
            x='points_per_game',
            y='points_allowed',
            backend='matplotlib',
            show_logos=False,
            add_reference_lines=True,
            reference_type='median',
            title='Test with Median Reference Lines'
        )
        
        self.assertIsInstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_team_stats_plot_with_mean_reference_lines(self):
        """Test plotting with mean reference lines."""
        fig = plot_team_stats(
            self.sample_team_data,
            x='points_per_game',
            y='points_allowed',
            backend='matplotlib',
            show_logos=False,
            add_reference_lines=True,
            reference_type='mean',
            title='Test with Mean Reference Lines'
        )
        
        self.assertIsInstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_team_stats_plot_with_both_reference_lines(self):
        """Test plotting with both median and mean reference lines."""
        fig = plot_team_stats(
            self.sample_team_data,
            x='offensive_epa',
            y='defensive_epa',
            backend='matplotlib',
            show_logos=False,
            add_reference_lines=True,
            reference_type='both',
            title='Test with Both Reference Lines'
        )
        
        self.assertIsInstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_custom_team_column_name(self):
        """Test using a custom team column name."""
        custom_data = self.sample_team_data.copy()
        custom_data = custom_data.rename(columns={'team': 'team_abbreviation'})
        
        fig = plot_team_stats(
            custom_data,
            x='offensive_epa',
            y='defensive_epa',
            team_column='team_abbreviation',
            backend='matplotlib',
            show_logos=False,
            title='Test with Custom Team Column'
        )
        
        self.assertIsInstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_invalid_backend_raises_error(self):
        """Test that invalid backend raises ValueError."""
        with self.assertRaises(ValueError):
            plot_team_stats(
                self.sample_team_data,
                x='offensive_epa',
                y='defensive_epa',
                backend='invalid_backend'
            )
    
    def test_missing_required_columns_raises_error(self):
        """Test that missing required columns raises ValueError."""
        incomplete_data = pd.DataFrame({
            'team': ['KC', 'GB'],
            'offensive_epa': [0.1, 0.05]
            # Missing 'defensive_epa'
        })
        
        with self.assertRaises(ValueError):
            plot_team_stats(
                incomplete_data,
                x='offensive_epa',
                y='defensive_epa',  # This column doesn't exist
                show_logos=False
            )


class test_color_functionality(TestCase):
    """Test NFL color functionality."""
    
    def test_get_team_colors_single_team(self):
        """Test getting color for a single team."""
        color = get_team_colors('KC', 'primary')
        self.assertIsInstance(color, str)
        self.assertTrue(color.startswith('#') or 'rgb' in color)
    
    def test_get_team_colors_multiple_teams(self):
        """Test getting colors for multiple teams."""
        teams = ['KC', 'GB', 'NE']
        colors = get_team_colors(teams, 'primary')
        
        self.assertIsInstance(colors, list)
        self.assertEqual(len(colors), len(teams))
        
        for color in colors:
            self.assertIsInstance(color, str)
            self.assertTrue(color.startswith('#') or 'rgb' in color)
    
    def test_get_team_colors_secondary(self):
        """Test getting secondary colors."""
        color = get_team_colors('KC', 'secondary')
        self.assertIsInstance(color, str)
    
    def test_color_palette_creation(self):
        """Test NFLColorPalette functionality."""
        palette = NFLColorPalette()
        
        # Test conference palette
        afc_colors = palette.create_conference_palette('AFC')
        self.assertIsInstance(afc_colors, dict)
        self.assertTrue(len(afc_colors) >= 16)  # AFC has 16 teams
        
        # Test division palette  
        nfc_west_colors = palette.create_division_palette('NFC West')
        self.assertIsInstance(nfc_west_colors, dict)
        self.assertEqual(len(nfc_west_colors), 4)  # Division has 4 teams


class test_utility_functions(TestCase):
    """Test utility functions."""
    
    def test_validate_teams(self):
        """Test team validation functionality."""
        valid_teams = ['KC', 'GB', 'NE']
        validated = validate_teams(valid_teams)
        self.assertIsInstance(validated, list)
        self.assertEqual(len(validated), len(valid_teams))
    
    def test_validate_teams_with_invalid_team(self):
        """Test validation with invalid teams."""
        mixed_teams = ['KC', 'INVALID', 'GB']
        # Should filter out invalid teams
        validated = validate_teams(mixed_teams, allow_conferences=False)
        self.assertIsInstance(validated, list)
        self.assertNotIn('INVALID', validated)
    
    def test_team_factor(self):
        """Test team factor functionality."""
        teams = ['KC', 'GB', 'NE', 'DAL']
        factor = team_factor(teams)
        
        # Should return some kind of categorical or factor representation
        self.assertIsNotNone(factor)
    
    def test_team_tiers(self):
        """Test team tiers functionality."""
        conference_tiers = team_tiers('conference')
        self.assertIsInstance(conference_tiers, dict)
        self.assertIn('AFC', conference_tiers)
        self.assertIn('NFC', conference_tiers)
        
        division_tiers = team_tiers('division')
        self.assertIsInstance(division_tiers, dict)
        # Should have 8 divisions
        self.assertTrue(len(division_tiers) >= 8)


class test_integration(TestCase):
    """Integration tests combining multiple features."""
    
    def test_end_to_end_visualization_pipeline(self):
        """Test complete visualization pipeline from data to plot."""
        # Simulate a complete workflow
        
        # 1. Create realistic team data
        np.random.seed(42)  # For reproducible tests
        teams = ['KC', 'BUF', 'GB', 'DAL', 'SF', 'NE', 'PIT', 'BAL']
        
        team_data = pd.DataFrame({
            'team': teams,
            'win_percentage': np.random.uniform(0.3, 0.9, len(teams)),
            'point_differential': np.random.normal(0, 8, len(teams)),
            'offensive_rating': np.random.normal(100, 15, len(teams)),
            'defensive_rating': np.random.normal(100, 12, len(teams))
        })
        
        # 2. Validate teams
        validated_teams = validate_teams(team_data['team'].tolist())
        self.assertTrue(len(validated_teams) > 0)
        
        # 3. Get colors for all teams
        colors = get_team_colors(validated_teams, 'primary')
        self.assertEqual(len(colors), len(validated_teams))
        
        # 4. Create visualization
        fig = plot_team_stats(
            team_data,
            x='offensive_rating',
            y='defensive_rating',
            backend='matplotlib',
            show_logos=False,  # Keep test fast
            add_reference_lines=True,
            reference_type='median',
            title='End-to-End Integration Test',
            figsize=(10, 8)
        )
        
        self.assertIsInstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_error_handling_in_pipeline(self):
        """Test that errors are handled gracefully in the pipeline."""
        # Test with some invalid data
        problematic_data = pd.DataFrame({
            'team': ['KC', 'INVALID_TEAM', 'GB'],
            'metric_x': [1, 2, 3],
            'metric_y': [4, 5, 6]
        })
        
        # Should not crash, should handle invalid teams gracefully
        try:
            fig = plot_team_stats(
                problematic_data,
                x='metric_x',
                y='metric_y',
                show_logos=False,
                title='Error Handling Test'
            )
            self.assertIsInstance(fig, plt.Figure)
            plt.close(fig)
        except Exception as e:
            # If it does raise an exception, it should be a clear, expected one
            self.assertIsInstance(e, (ValueError, KeyError))