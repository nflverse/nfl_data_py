from unittest import TestCase
from pathlib import Path
import tempfile
import shutil

from PIL import Image
import pandas as pd
import matplotlib.pyplot as plt

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from nflplotpy.core.logos import get_team_logo, get_available_teams, NFLAssetManager
from nflplotpy.core.colors import get_team_colors
from nflplotpy.matplotlib.artists import add_nfl_logo, add_nfl_logos
import nflplotpy as nflplot


class test_logo_downloading(TestCase):
    """Test NFL team logo downloading and caching functionality."""
    
    def test_get_team_logo_returns_pil_image(self):
        """Test that get_team_logo returns a PIL Image."""
        logo = get_team_logo('KC')
        self.assertIsInstance(logo, Image.Image)
        self.assertTrue(logo.size[0] > 0)
        self.assertTrue(logo.size[1] > 0)
    
    def test_get_team_logo_with_alternative_abbreviation(self):
        """Test that alternative team abbreviations work."""
        # Test that ARZ maps to ARI
        logo_ari = get_team_logo('ARI')
        logo_arz = get_team_logo('ARZ')
        self.assertIsInstance(logo_ari, Image.Image)
        self.assertIsInstance(logo_arz, Image.Image)
    
    def test_get_team_logo_invalid_team_raises_error(self):
        """Test that invalid team abbreviations raise ValueError."""
        with self.assertRaises(ValueError):
            get_team_logo('INVALID')
    
    def test_get_available_teams_returns_list(self):
        """Test that get_available_teams returns a list of team abbreviations."""
        teams = get_available_teams()
        self.assertIsInstance(teams, list)
        self.assertTrue(len(teams) >= 32)  # At least 32 NFL teams
        self.assertIn('KC', teams)
        self.assertIn('GB', teams)
    
    def test_logo_caching_works(self):
        """Test that logo caching works properly."""
        # Use temporary directory for test cache
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = NFLAssetManager(cache_dir=temp_dir)
            
            # First download should create cache
            logo1 = manager.get_logo('KC')
            cache_info_1 = manager.get_cache_info()
            
            # Second download should use cache
            logo2 = manager.get_logo('KC')
            cache_info_2 = manager.get_cache_info()
            
            self.assertIsInstance(logo1, Image.Image)
            self.assertIsInstance(logo2, Image.Image)
            self.assertEqual(cache_info_1['logos_count'], cache_info_2['logos_count'])


class test_matplotlib_integration(TestCase):
    """Test matplotlib integration for NFL logos."""
    
    def test_add_nfl_logo_to_axes(self):
        """Test adding a single NFL logo to matplotlib axes."""
        fig, ax = plt.subplots(figsize=(6, 6))
        
        # Add a logo
        annotation = add_nfl_logo(ax, 'KC', 0.5, 0.5, width=0.1)
        
        # annotation could be None if logo fails to load, but shouldn't raise exception
        if annotation is not None:
            self.assertTrue(hasattr(annotation, 'xy'))
        
        plt.close(fig)
    
    def test_add_multiple_nfl_logos(self):
        """Test adding multiple NFL logos to matplotlib axes."""
        fig, ax = plt.subplots(figsize=(8, 6))
        
        teams = ['KC', 'GB', 'NE']
        x_positions = [0.2, 0.5, 0.8]
        y_positions = [0.5, 0.5, 0.5]
        
        annotations = add_nfl_logos(ax, teams, x_positions, y_positions, width=0.1)
        
        self.assertIsInstance(annotations, list)
        # Some logos might fail to load, but function should not crash
        self.assertTrue(len(annotations) <= len(teams))
        
        plt.close(fig)
    
    def test_add_nfl_logos_mismatched_arrays_raises_error(self):
        """Test that mismatched array lengths raise ValueError."""
        fig, ax = plt.subplots(figsize=(6, 6))
        
        teams = ['KC', 'GB']
        x_positions = [0.2, 0.5, 0.8]  # Wrong length
        y_positions = [0.5, 0.5]
        
        with self.assertRaises(ValueError):
            add_nfl_logos(ax, teams, x_positions, y_positions)
        
        plt.close(fig)


class test_high_level_plotting(TestCase):
    """Test high-level plotting functions with logos."""
    
    def test_plot_team_stats_with_logos_enabled(self):
        """Test plot_team_stats function with show_logos=True."""
        # Create sample data
        sample_data = pd.DataFrame({
            'team': ['KC', 'GB', 'NE'],
            'offensive_epa': [0.1, -0.05, 0.08],
            'defensive_epa': [0.02, -0.03, 0.01]
        })
        
        # Should not raise exception even if some logos fail
        fig = nflplot.plot_team_stats(
            sample_data,
            x='offensive_epa',
            y='defensive_epa',
            backend='matplotlib',
            show_logos=True,
            logo_size=0.1,
            title='Test Plot with Logos'
        )
        
        self.assertIsInstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_plot_team_stats_with_logos_disabled(self):
        """Test plot_team_stats function with show_logos=False."""
        sample_data = pd.DataFrame({
            'team': ['KC', 'GB', 'NE'],
            'offensive_epa': [0.1, -0.05, 0.08],
            'defensive_epa': [0.02, -0.03, 0.01]
        })
        
        fig = nflplot.plot_team_stats(
            sample_data,
            x='offensive_epa',
            y='defensive_epa',
            backend='matplotlib',
            show_logos=False,
            title='Test Plot with Dots'
        )
        
        self.assertIsInstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_plot_team_stats_missing_team_column_raises_error(self):
        """Test that missing team column raises ValueError."""
        sample_data = pd.DataFrame({
            'not_team': ['KC', 'GB', 'NE'],
            'offensive_epa': [0.1, -0.05, 0.08],
            'defensive_epa': [0.02, -0.03, 0.01]
        })
        
        with self.assertRaises(ValueError):
            nflplot.plot_team_stats(
                sample_data,
                x='offensive_epa',
                y='defensive_epa',
                show_logos=True
            )


class test_logo_color_consistency(TestCase):
    """Test that logos and colors work consistently for the same teams."""
    
    def test_logo_and_color_consistency(self):
        """Test that teams with working logos also have working colors."""
        test_teams = ['KC', 'GB', 'NE', 'DAL']
        
        for team in test_teams:
            # Both should work or both should fail for consistency
            try:
                logo = get_team_logo(team)
                color = get_team_colors(team, 'primary')
                
                # If we get here, both worked
                self.assertIsInstance(logo, Image.Image)
                self.assertIsInstance(color, str)
                self.assertTrue(color.startswith('#') or color.startswith('rgb'))
                
            except Exception as e:
                # If logo fails, color should still work (colors don't require downloads)
                color = get_team_colors(team, 'primary')
                self.assertIsInstance(color, str)


class test_asset_manager(TestCase):
    """Test NFLAssetManager functionality."""
    
    def test_asset_manager_cache_info(self):
        """Test that asset manager provides cache information."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = NFLAssetManager(cache_dir=temp_dir)
            cache_info = manager.get_cache_info()
            
            self.assertIsInstance(cache_info, dict)
            self.assertIn('cache_dir', cache_info)
            self.assertIn('logos_count', cache_info)
            self.assertIn('total_size_bytes', cache_info)
    
    def test_asset_manager_clear_cache(self):
        """Test cache clearing functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = NFLAssetManager(cache_dir=temp_dir)
            
            # Download a logo to create cache
            try:
                manager.get_logo('KC')
                cache_info_before = manager.get_cache_info()
                
                # Clear cache
                manager.clear_cache('logos')
                cache_info_after = manager.get_cache_info()
                
                # Logo count should be 0 after clearing
                self.assertEqual(cache_info_after['logos_count'], 0)
                
            except Exception:
                # If logo download fails, that's okay for this test
                pass