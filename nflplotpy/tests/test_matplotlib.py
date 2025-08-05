"""Tests for matplotlib integration."""

import pytest
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock

from nflplotpy.matplotlib.artists import (
    add_nfl_logo, add_nfl_logos, add_median_lines, add_mean_lines
)
from nflplotpy.matplotlib.scales import (
    nfl_color_scale, apply_nfl_theme, set_team_colors,
    create_team_scatter_colors, add_team_color_legend
)


class TestMatplotlibArtists:
    """Test matplotlib artists and logo functionality."""
    
    def test_add_median_lines(self):
        """Test adding median reference lines."""
        fig, ax = plt.subplots()
        data = [1, 2, 3, 4, 5]
        
        add_median_lines(ax, data, axis='both')
        
        # Check that lines were added
        lines = ax.get_lines()
        assert len(lines) >= 2  # At least x and y median lines
        
        plt.close(fig)
    
    def test_add_mean_lines(self):
        """Test adding mean reference lines."""
        fig, ax = plt.subplots()
        data = [1, 2, 3, 4, 5]
        
        add_mean_lines(ax, data, axis='y')
        
        # Check that horizontal line was added
        lines = ax.get_lines()
        assert len(lines) >= 1
        
        plt.close(fig)
    
    @patch('nflplotpy.matplotlib.artists.get_team_logo')
    def test_add_nfl_logo_mock(self, mock_get_logo):
        """Test adding NFL logo with mocked image."""
        # Mock PIL Image
        mock_image = MagicMock()
        mock_image.__array__ = MagicMock(return_value=np.ones((100, 100, 4)))
        mock_get_logo.return_value = mock_image
        
        fig, ax = plt.subplots()
        
        # Should not raise error
        result = add_nfl_logo(ax, 'ARI', 0.5, 0.5)
        
        # Should have called get_team_logo
        mock_get_logo.assert_called_once_with('ARI')
        
        plt.close(fig)
    
    @patch('nflplotpy.matplotlib.artists.get_team_logo')
    def test_add_nfl_logos_mock(self, mock_get_logo):
        """Test adding multiple NFL logos."""
        mock_image = MagicMock()
        mock_image.__array__ = MagicMock(return_value=np.ones((100, 100, 4)))
        mock_get_logo.return_value = mock_image
        
        fig, ax = plt.subplots()
        teams = ['ARI', 'ATL']
        x = [0.3, 0.7]
        y = [0.5, 0.5]
        
        results = add_nfl_logos(ax, teams, x, y)
        
        assert len(results) == 2
        assert mock_get_logo.call_count == 2
        
        plt.close(fig)
    
    def test_add_nfl_logos_invalid_input(self):
        """Test error handling for invalid inputs."""
        fig, ax = plt.subplots()
        
        with pytest.raises(ValueError):
            add_nfl_logos(ax, ['ARI', 'ATL'], [0.5], [0.5, 0.6])  # Mismatched lengths
        
        plt.close(fig)


class TestMatplotlibScales:
    """Test matplotlib scales and theming."""
    
    def test_nfl_color_scale(self):
        """Test NFL color scale creation."""
        teams = ['ARI', 'ATL', 'BAL']
        cmap = nfl_color_scale(teams)
        
        assert isinstance(cmap, mcolors.ListedColormap)
        assert len(cmap.colors) == len(teams)
    
    def test_apply_nfl_theme(self):
        """Test applying NFL theme."""
        fig, ax = plt.subplots()
        
        # Should not raise error
        apply_nfl_theme(ax, style='default')
        apply_nfl_theme(ax, style='minimal')
        apply_nfl_theme(ax, team='ARI')
        
        with pytest.raises(ValueError):
            apply_nfl_theme(ax, style='invalid')
        
        plt.close(fig)
    
    def test_set_team_colors(self):
        """Test setting team colors."""
        fig, ax = plt.subplots()
        teams = ['ARI', 'ATL']
        
        color_map = set_team_colors(ax, teams)
        
        assert isinstance(color_map, dict)
        assert len(color_map) == len(teams)
        for team in teams:
            assert team in color_map
            assert color_map[team].startswith('#')
        
        plt.close(fig)
    
    def test_create_team_scatter_colors(self):
        """Test creating scatter plot colors."""
        teams = ['ARI', 'ATL', 'BAL']
        
        # Test with team colors only
        colors = create_team_scatter_colors(teams)
        assert len(colors) == len(teams)
        for color in colors:
            assert color.startswith('#')
        
        # Test with values
        values = [1.0, 2.0, 3.0]
        colors = create_team_scatter_colors(teams, values)
        assert len(colors) == len(teams)
    
    def test_add_team_color_legend(self):
        """Test adding team color legend."""
        fig, ax = plt.subplots()
        teams = ['ARI', 'ATL']
        
        legend = add_team_color_legend(ax, teams)
        
        assert legend is not None
        # Check legend has correct number of entries
        assert len(legend.get_texts()) == len(teams)
        
        plt.close(fig)


class TestIntegration:
    """Integration tests for matplotlib functionality."""
    
    def test_complete_plot_creation(self):
        """Test creating a complete plot with NFL styling."""
        # Sample data
        teams = ['ARI', 'ATL', 'BAL']
        x = [1, 2, 3]
        y = [2, 1, 3]
        
        fig, ax = plt.subplots(figsize=(8, 6))
        
        # Create scatter plot with team colors
        color_map = set_team_colors(ax, teams)
        colors = [color_map[team] for team in teams]
        
        ax.scatter(x, y, c=colors, s=100, alpha=0.8)
        
        # Add reference lines
        add_median_lines(ax, x, axis='x')
        add_mean_lines(ax, y, axis='y')
        
        # Apply NFL theme
        apply_nfl_theme(ax, style='default')
        
        # Add legend
        add_team_color_legend(ax, teams)
        
        # Styling
        ax.set_xlabel('X Metric')
        ax.set_ylabel('Y Metric')
        ax.set_title('NFL Team Comparison')
        
        # Should complete without error
        plt.tight_layout()
        plt.close(fig)
    
    def test_theme_variations(self):
        """Test different theme variations."""
        fig, axes = plt.subplots(1, 3, figsize=(15, 5))
        
        styles = ['default', 'minimal', 'dark']
        
        for ax, style in zip(axes, styles):
            # Sample plot
            ax.scatter([1, 2, 3], [1, 2, 3])
            apply_nfl_theme(ax, style=style, team='ARI')
            ax.set_title(f'{style.title()} Theme')
        
        plt.tight_layout()
        plt.close(fig)
    
    @patch('nflplotpy.matplotlib.artists.get_team_logo')
    def test_logo_scatter_plot(self, mock_get_logo):
        """Test scatter plot with logos instead of points."""
        mock_image = MagicMock()
        mock_image.__array__ = MagicMock(return_value=np.ones((50, 50, 4)))
        mock_get_logo.return_value = mock_image
        
        fig, ax = plt.subplots()
        
        teams = ['ARI', 'ATL']
        x = [1, 2]
        y = [1, 2]
        
        # Create invisible scatter for positioning
        ax.scatter(x, y, alpha=0.01, s=1)
        
        # Add logos
        add_nfl_logos(ax, teams, x, y, width=0.1)
        
        # Style
        apply_nfl_theme(ax)
        ax.set_xlim(0, 3)
        ax.set_ylim(0, 3)
        
        plt.close(fig)


if __name__ == "__main__":
    pytest.main([__file__])