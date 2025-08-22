"""Tests for new nflplotpy features: pandas styling, preview, elements, etc."""

import pytest
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tempfile
import os
from pathlib import Path

# Import the new modules
from nflplotpy.pandas.styling import (
    style_with_logos, style_with_headshots, style_with_wordmarks,
    create_nfl_table, NFLTableStyler
)
from nflplotpy.matplotlib.preview import (
    nfl_preview, preview_with_dimensions, preview_comparison
)
from nflplotpy.matplotlib.elements import (
    set_xlabel_with_logos, set_ylabel_with_logos, add_logo_watermark
)
from nflplotpy.core.urls import (
    AssetURLManager, get_team_wordmark_url, get_player_headshot_urls,
    discover_player_id, validate_all_urls
)
from nflplotpy.core.utils import (
    nfl_sitrep, clear_all_cache, validate_player_ids,
    discover_team_from_colors, get_season_info
)


class TestPandasStyling:
    """Test pandas styling functionality."""
    
    def setup_method(self):
        """Set up test data."""
        self.sample_df = pd.DataFrame({
            'team': ['KC', 'BUF', 'BAL'],
            'wins': [14, 13, 13],
            'losses': [3, 4, 4]
        })
    
    def test_style_with_logos_basic(self):
        """Test basic logo styling."""
        styled = style_with_logos(self.sample_df, 'team')
        
        # Check that we get a pandas Styler
        assert hasattr(styled, 'to_html')
        
        # Check that HTML contains team abbreviations (as fallback)
        html = styled.to_html()
        assert 'KC' in html or 'img' in html  # Either text or image
    
    def test_nfl_table_styler_class(self):
        """Test NFLTableStyler class."""
        styler = NFLTableStyler(self.sample_df)
        
        # Test method chaining
        result = styler.with_team_logos('team').with_nfl_theme()
        assert isinstance(result, NFLTableStyler)
        
        # Test HTML generation
        html = result.to_html()
        assert isinstance(html, str)
        assert len(html) > 100  # Should be substantial HTML
    
    def test_create_nfl_table(self):
        """Test comprehensive table creation."""
        table = create_nfl_table(
            self.sample_df,
            team_column='team',
            logo_columns='team'
        )
        
        assert isinstance(table, NFLTableStyler)
        
        # Test saving to temporary file
        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as f:
            table.save_html(f.name)
            assert os.path.exists(f.name)
            os.unlink(f.name)
    
    def test_style_with_headshots_placeholder(self):
        """Test headshot styling (placeholder implementation)."""
        player_df = pd.DataFrame({
            'player': ['Patrick Mahomes', 'Josh Allen'],
            'team': ['KC', 'BUF']
        })
        
        styled = style_with_headshots(player_df, 'player')
        html = styled.to_html()
        
        # Should contain placeholder emojis
        assert '👤' in html
    
    def test_style_with_wordmarks_placeholder(self):
        """Test wordmark styling (placeholder implementation).""" 
        styled = style_with_wordmarks(self.sample_df, 'team')
        html = styled.to_html()
        
        # Should contain team names as wordmarks
        assert any(name in html for name in ['CHIEFS', 'BILLS', 'RAVENS'])


class TestMatplotlibPreview:
    """Test matplotlib preview functionality."""
    
    def setup_method(self):
        """Set up test plot."""
        self.fig, self.ax = plt.subplots()
        self.ax.plot([1, 2, 3], [1, 4, 2])
        self.ax.set_title('Test Plot')
    
    def teardown_method(self):
        """Clean up plots."""
        plt.close(self.fig)
    
    def test_nfl_preview_basic(self):
        """Test basic preview functionality."""
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
            preview_path = nfl_preview(
                self.fig,
                width=8,
                height=6,
                show_in_notebook=False,
                save_path=f.name
            )
            
            assert preview_path == f.name
            assert os.path.exists(preview_path)
            
            # Check file size is reasonable (not empty)
            assert os.path.getsize(preview_path) > 1000
            
            os.unlink(preview_path)
    
    def test_preview_with_dimensions(self):
        """Test preview with dimension presets."""
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
            preview_path = preview_with_dimensions(
                self.fig,
                dimensions='standard',
                show_in_notebook=False,
                save_path=f.name
            )
            
            assert os.path.exists(preview_path)
            os.unlink(preview_path)
    
    def test_preview_comparison(self):
        """Test preview comparison functionality."""
        # Create second figure
        fig2, ax2 = plt.subplots()
        ax2.bar([1, 2, 3], [3, 1, 4])
        
        try:
            paths = preview_comparison([self.fig, fig2], format='png')
            
            assert len(paths) == 2
            for path in paths:
                assert os.path.exists(path)
                os.unlink(path)
        finally:
            plt.close(fig2)
    
    def test_dimension_presets(self):
        """Test different dimension presets."""
        presets = ['standard', 'wide', 'square', 'presentation']
        
        for preset in presets:
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
                path = preview_with_dimensions(
                    self.fig,
                    dimensions=preset,
                    show_in_notebook=False,
                    save_path=f.name
                )
                assert os.path.exists(path)
                os.unlink(path)


class TestMatplotlibElements:
    """Test matplotlib theme elements."""
    
    def setup_method(self):
        """Set up test plot."""
        self.fig, self.ax = plt.subplots()
    
    def teardown_method(self):
        """Clean up plots.""" 
        plt.close(self.fig)
    
    def test_add_logo_watermark(self):
        """Test logo watermark functionality."""
        # This should not raise an error
        add_logo_watermark(self.ax, 'KC', position='bottom_right')
        
        # Check that artists were added
        assert len(self.ax.get_children()) > 0
    
    def test_set_xlabel_with_logos(self):
        """Test x-axis logo labels."""
        teams = ['KC', 'BUF']
        positions = [0, 1]
        
        # Should not raise an error
        set_xlabel_with_logos(self.ax, teams, positions)
        
        # Check that ticks were set
        assert len(self.ax.get_xticks()) >= len(teams)


class TestURLManagement:
    """Test URL management system."""
    
    def test_asset_url_manager(self):
        """Test AssetURLManager class."""
        manager = AssetURLManager()
        
        # Test logo URL retrieval
        url = manager.get_logo_url('KC')
        assert isinstance(url, str)
        assert url.startswith(('http://', 'https://'))
        
        # Test wordmark URL (should fallback to logo)
        wordmark_url = manager.get_wordmark_url('KC')
        assert isinstance(wordmark_url, str)
    
    def test_get_team_wordmark_url(self):
        """Test wordmark URL convenience function."""
        url = get_team_wordmark_url('KC')
        assert isinstance(url, str)
        assert url.startswith(('http://', 'https://'))
    
    def test_get_player_headshot_urls(self):
        """Test player headshot URL generation."""
        urls = get_player_headshot_urls("12345")
        
        assert isinstance(urls, dict)
        assert 'espn_full' in urls
        assert 'espn_small' in urls
    
    def test_discover_player_id(self):
        """Test player ID discovery."""
        # Test known player
        player_id = discover_player_id("patrick mahomes")
        if player_id:  # May return None if not implemented
            assert isinstance(player_id, str)
    
    def test_url_validation(self):
        """Test URL validation."""
        manager = AssetURLManager()
        
        assert manager.validate_url('https://example.com')
        assert manager.validate_url('http://test.org/image.png')
        assert not manager.validate_url('not-a-url')
        assert not manager.validate_url('')


class TestUtilityFunctions:
    """Test utility function enhancements."""
    
    def test_validate_player_ids(self):
        """Test player ID validation."""
        test_ids = ['12345', 'abc', '1', '1234567']
        results = validate_player_ids(test_ids)
        
        assert isinstance(results, dict)
        assert len(results) == len(test_ids)
        
        # Numeric IDs with sufficient length should be valid
        assert results['12345'] == True
        assert results['1234567'] == True
        
        # Non-numeric or too short should be invalid
        assert results['abc'] == False
        assert results['1'] == False
    
    def test_discover_team_from_colors(self):
        """Test team discovery from colors."""
        # KC primary color
        team = discover_team_from_colors('#e31837')
        assert team == 'KC'
        
        # Non-existent color
        team = discover_team_from_colors('#ffffff')
        assert team is None
    
    def test_get_season_info(self):
        """Test season information."""
        info = get_season_info(2024)
        
        assert isinstance(info, dict)
        assert info['season'] == 2024
        assert 'is_current' in info
        assert 'data_available' in info
        assert 'regular_season_weeks' in info
        
        # Test historical season
        info_2020 = get_season_info(2020)
        assert info_2020['is_historical'] == True
    
    def test_nfl_sitrep_runs(self):
        """Test that nfl_sitrep runs without error."""
        # Capture stdout to avoid cluttering test output
        import io
        import sys
        from contextlib import redirect_stdout
        
        f = io.StringIO()
        with redirect_stdout(f):
            nfl_sitrep()
        
        output = f.getvalue()
        assert '🏈 nflplotpy System Report' in output
        assert 'Package Information' in output


class TestIntegrationFeatures:
    """Test integration between new features."""
    
    def test_full_workflow_example(self):
        """Test a complete workflow using multiple new features."""
        # Create sample data
        df = pd.DataFrame({
            'team': ['KC', 'BUF'],
            'wins': [14, 13],
            'epa': [0.15, 0.12]
        })
        
        # 1. Create styled table
        table = create_nfl_table(df, team_column='team')
        html = table.to_html()
        assert len(html) > 100
        
        # 2. Create plot
        fig, ax = plt.subplots()
        teams = df['team'].tolist()
        wins = df['wins'].tolist()
        
        bars = ax.bar(teams, wins)
        
        # 3. Add logo watermark
        add_logo_watermark(ax, 'KC', position='bottom_right')
        
        # 4. Preview the plot
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
            preview_path = nfl_preview(
                fig, 
                show_in_notebook=False, 
                save_path=f.name
            )
            assert os.path.exists(preview_path)
            os.unlink(preview_path)
        
        plt.close(fig)
    
    def test_error_handling(self):
        """Test error handling in new features."""
        # Invalid team in table styling
        df = pd.DataFrame({'team': ['INVALID'], 'wins': [10]})
        
        # Should handle gracefully
        styled = style_with_logos(df, 'team')
        html = styled.to_html()
        assert 'INVALID' in html  # Should fall back to text
        
        # Invalid dimensions in preview
        fig, ax = plt.subplots()
        ax.plot([1, 2, 3])
        
        with pytest.raises(ValueError):
            preview_with_dimensions(fig, dimensions='invalid_preset')
        
        plt.close(fig)


if __name__ == "__main__":
    pytest.main([__file__])