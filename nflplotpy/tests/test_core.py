"""Tests for core nflplotpy functionality."""

import pytest
import pandas as pd
import numpy as np
from PIL import Image
import tempfile
import os

from nflplotpy.core.logos import (
    NFL_TEAM_LOGOS, normalize_team_abbreviation, 
    get_team_logo_url, get_available_teams, get_asset_manager
)
from nflplotpy.core.colors import (
    NFL_TEAM_COLORS, NFLColorPalette, get_team_colors, 
    get_palette_manager, create_nfl_colormap
)
from nflplotpy.core.utils import (
    team_factor, team_tiers, validate_teams, 
    get_team_info, clean_team_abbreviations
)
from nflplotpy.core.assets import NFLAssetManager


class TestLogos:
    """Test logo functionality."""
    
    def test_nfl_team_logos_data(self):
        """Test that NFL_TEAM_LOGOS contains expected data."""
        assert isinstance(NFL_TEAM_LOGOS, dict)
        assert len(NFL_TEAM_LOGOS) > 30  # At least 32 teams + conferences
        
        # Check specific teams
        assert 'ARI' in NFL_TEAM_LOGOS
        assert 'NE' in NFL_TEAM_LOGOS
        assert 'AFC' in NFL_TEAM_LOGOS
        assert 'NFC' in NFL_TEAM_LOGOS
        assert 'NFL' in NFL_TEAM_LOGOS
        
        # Check URLs are strings
        for team, url in NFL_TEAM_LOGOS.items():
            assert isinstance(url, str)
            assert url.startswith(('http://', 'https://'))
    
    def test_normalize_team_abbreviation(self):
        """Test team abbreviation normalization."""
        # Direct matches
        assert normalize_team_abbreviation('ARI') == 'ARI'
        assert normalize_team_abbreviation('ari') == 'ARI'
        
        # Mappings
        assert normalize_team_abbreviation('ARZ') == 'ARI'
        assert normalize_team_abbreviation('GNB') == 'GB'
        
        # Invalid team
        with pytest.raises(ValueError):
            normalize_team_abbreviation('INVALID')
    
    def test_get_team_logo_url(self):
        """Test getting team logo URLs."""
        url = get_team_logo_url('ARI')
        assert isinstance(url, str)
        assert url.startswith(('http://', 'https://'))
        
        # Test with mapping
        url = get_team_logo_url('ARZ')
        assert isinstance(url, str)
    
    def test_get_available_teams(self):
        """Test getting available teams."""
        teams = get_available_teams()
        assert isinstance(teams, list)
        assert len(teams) == 32  # 32 NFL teams
        assert 'ARI' in teams
        assert 'AFC' not in teams  # Conferences excluded
    
    def test_asset_manager(self):
        """Test asset manager functionality."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = NFLAssetManager(cache_dir=tmpdir)
            
            # Test cache info
            info = manager.get_cache_info()
            assert isinstance(info, dict)
            assert 'cache_dir' in info
            assert 'logos_count' in info


class TestColors:
    """Test color functionality."""
    
    def test_nfl_team_colors_data(self):
        """Test that NFL_TEAM_COLORS contains expected data."""
        assert isinstance(NFL_TEAM_COLORS, dict)
        assert len(NFL_TEAM_COLORS) > 30
        
        # Check specific team
        assert 'ARI' in NFL_TEAM_COLORS
        colors = NFL_TEAM_COLORS['ARI']
        assert isinstance(colors, dict)
        assert 'primary' in colors
        assert 'secondary' in colors
        
        # Check hex color format
        assert colors['primary'].startswith('#')
        assert len(colors['primary']) == 7
    
    def test_nfl_color_palette(self):
        """Test NFLColorPalette class."""
        palette = NFLColorPalette()
        
        # Test single team
        color = palette.get_team_colors('ARI', 'primary')
        assert isinstance(color, str)
        assert color.startswith('#')
        
        # Test multiple teams
        colors = palette.get_team_colors(['ARI', 'ATL'], 'primary')
        assert isinstance(colors, list)
        assert len(colors) == 2
        
        # Test invalid color type
        with pytest.raises(ValueError):
            palette.get_team_colors('ARI', 'invalid')
    
    def test_get_team_colors(self):
        """Test convenience function."""
        color = get_team_colors('ARI', 'primary')
        assert isinstance(color, str)
        
        colors = get_team_colors(['ARI', 'ATL'])
        assert isinstance(colors, list)
    
    def test_create_nfl_colormap(self):
        """Test matplotlib colormap creation."""
        teams = ['ARI', 'ATL', 'BAL']
        cmap = create_nfl_colormap(teams)
        
        # Check it's a matplotlib colormap
        assert hasattr(cmap, 'colors')
        assert len(cmap.colors) == len(teams)
    
    def test_gradient_creation(self):
        """Test color gradient creation."""
        palette = get_palette_manager()
        gradient = palette.create_gradient('ARI', 'ATL', n_colors=5)
        
        assert isinstance(gradient, list)
        assert len(gradient) == 5
        for color in gradient:
            assert color.startswith('#')


class TestUtils:
    """Test utility functions."""
    
    def test_team_factor(self):
        """Test team factor creation."""
        teams = ['ARI', 'ATL', 'BAL']
        factor = team_factor(teams)
        
        assert isinstance(factor, pd.Categorical)
        assert len(factor) == 3
        assert factor.ordered
    
    def test_team_tiers(self):
        """Test team tier creation."""
        tiers = team_tiers(method='conference')
        assert isinstance(tiers, dict)
        assert 'AFC' in tiers
        assert 'NFC' in tiers
        
        tiers = team_tiers(method='division')
        assert len(tiers) == 8  # 8 divisions
        
        with pytest.raises(ValueError):
            team_tiers(method='invalid')
    
    def test_validate_teams(self):
        """Test team validation."""
        teams = validate_teams(['ARI', 'ATL'])
        assert teams == ['ARI', 'ATL']
        
        teams = validate_teams('ari')
        assert teams == ['ARI']
        
        with pytest.raises(ValueError):
            validate_teams(['INVALID'])
    
    def test_get_team_info(self):
        """Test team info retrieval."""
        info = get_team_info(['ARI', 'ATL'])
        assert isinstance(info, pd.DataFrame)
        assert len(info) == 2
        assert 'team_abbr' in info.columns
        assert 'primary_color' in info.columns
    
    def test_clean_team_abbreviations(self):
        """Test team abbreviation cleaning."""
        df = pd.DataFrame({
            'team': ['ari', 'ARZ', 'ATL'],
            'value': [1, 2, 3]
        })
        
        cleaned = clean_team_abbreviations(df, 'team')
        assert cleaned['team'].tolist() == ['ARI', 'ARI', 'ATL']


class TestAssets:
    """Test asset management."""
    
    def test_asset_manager_init(self):
        """Test asset manager initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = NFLAssetManager(cache_dir=tmpdir)
            
            assert manager.cache_dir.exists()
            assert manager.logos_dir.exists()
            assert manager.headshots_dir.exists()
            assert manager.wordmarks_dir.exists()
    
    def test_cache_operations(self):
        """Test cache operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = NFLAssetManager(cache_dir=tmpdir)
            
            # Test cache info
            info = manager.get_cache_info()
            assert info['logos_count'] == 0
            
            # Test cache clearing
            manager.clear_cache()
            # Should not error even with empty cache


class TestIntegration:
    """Integration tests."""
    
    def test_package_imports(self):
        """Test that main package imports work."""
        import nflplotpy
        
        # Test main components are available
        assert hasattr(nflplotpy, 'NFLAssetManager')
        assert hasattr(nflplotpy, 'NFLColorPalette')
        assert hasattr(nflplotpy, 'add_nfl_logo')
        assert hasattr(nflplotpy, 'get_team_colors')
    
    @pytest.mark.parametrize("team", ['ARI', 'ATL', 'BAL', 'BUF', 'CAR'])
    def test_team_data_consistency(self, team):
        """Test that team data is consistent across modules."""
        # Team should exist in all data structures
        assert team in NFL_TEAM_LOGOS
        assert team in NFL_TEAM_COLORS
        
        # Should be able to get colors and logo URL
        color = get_team_colors(team)
        assert isinstance(color, str)
        
        url = get_team_logo_url(team)
        assert isinstance(url, str)
    
    def test_data_sample_integration(self):
        """Test with sample NFL-like data."""
        # Create sample data similar to nfl_data_py output
        sample_data = pd.DataFrame({
            'team': ['ARI', 'ATL', 'BAL'],
            'epa_per_play': [0.1, -0.05, 0.08],
            'success_rate': [0.45, 0.42, 0.47]
        })
        
        # Test team info integration
        info = get_team_info(sample_data['team'].tolist())
        assert len(info) == 3
        
        # Test color retrieval
        colors = get_team_colors(sample_data['team'].tolist())
        assert len(colors) == 3
        
        # Test team factor
        factor = team_factor(sample_data['team'])
        assert len(factor) == 3


if __name__ == "__main__":
    pytest.main([__file__])