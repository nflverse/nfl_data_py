"""Tests for high-level plotting functions."""

import pytest
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from nflplotpy.core.plotting import (
    plot_team_stats, plot_player_comparison, 
    _create_radar_chart, _create_player_bar_chart
)


class TestHighLevelPlotting:
    """Test high-level plotting functions."""
    
    def setUp_sample_team_data(self):
        """Create sample team data for testing."""
        return pd.DataFrame({
            'team': ['ARI', 'ATL', 'BAL', 'BUF'],
            'epa_per_play': [0.05, -0.02, 0.08, 0.03],
            'success_rate': [0.45, 0.42, 0.48, 0.44],
            'yards_per_play': [5.2, 4.8, 5.5, 5.0]
        })
    
    def setUp_sample_player_data(self):
        """Create sample player data for testing."""
        return pd.DataFrame({
            'player_display_name': ['Josh Allen', 'Lamar Jackson', 'Tom Brady'],
            'recent_team': ['BUF', 'BAL', 'TB'],
            'passing_yards': [4544, 3678, 4633],
            'passing_tds': [37, 26, 40],
            'qbr': [70.3, 64.2, 78.1],
            'completion_percentage': [63.3, 64.4, 65.7]
        })
    
    @patch('nflplotpy.core.plotting.add_nfl_logos')
    @patch('nflplotpy.core.plotting.get_team_colors')
    def test_plot_team_stats_matplotlib(self, mock_colors, mock_logos):
        """Test matplotlib team stats plotting."""
        mock_colors.return_value = ['#97233f', '#a71930', '#241773', '#00338d']
        
        data = self.setUp_sample_team_data()
        
        fig = plot_team_stats(
            data, 
            x='epa_per_play', 
            y='success_rate',
            backend='matplotlib',
            show_logos=True
        )
        
        assert isinstance(fig, plt.Figure)
        mock_logos.assert_called_once()
        
        plt.close(fig)
    
    def test_plot_team_stats_no_logos(self):
        """Test team stats plot without logos."""
        data = self.setUp_sample_team_data()
        
        fig = plot_team_stats(
            data,
            x='epa_per_play',
            y='success_rate', 
            backend='matplotlib',
            show_logos=False
        )
        
        assert isinstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_plot_team_stats_invalid_columns(self):
        """Test error handling for invalid columns."""
        data = self.setUp_sample_team_data()
        
        with pytest.raises(ValueError):
            plot_team_stats(data, x='invalid_column', y='success_rate')
    
    def test_plot_team_stats_invalid_backend(self):
        """Test error handling for invalid backend."""
        data = self.setUp_sample_team_data()
        
        with pytest.raises(ValueError):
            plot_team_stats(data, x='epa_per_play', y='success_rate', backend='invalid')
    
    def test_plot_player_comparison_radar(self):
        """Test player comparison radar chart."""
        data = self.setUp_sample_player_data()
        players = ['Josh Allen', 'Lamar Jackson']
        metrics = ['passing_yards', 'passing_tds', 'qbr']
        
        fig = plot_player_comparison(
            data, 
            players=players,
            metrics=metrics,
            plot_type='radar'
        )
        
        assert isinstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_plot_player_comparison_bar(self):
        """Test player comparison bar chart."""
        data = self.setUp_sample_player_data()
        players = ['Josh Allen', 'Tom Brady']
        metrics = ['passing_yards', 'passing_tds']
        
        fig = plot_player_comparison(
            data,
            players=players, 
            metrics=metrics,
            plot_type='bar'
        )
        
        assert isinstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_plot_player_comparison_invalid_players(self):
        """Test error handling for invalid players."""
        data = self.setUp_sample_player_data()
        
        with pytest.raises(ValueError):
            plot_player_comparison(
                data,
                players=['Nonexistent Player'],
                metrics=['passing_yards']
            )
    
    def test_plot_player_comparison_invalid_metrics(self):
        """Test error handling for invalid metrics."""
        data = self.setUp_sample_player_data()
        
        with pytest.raises(ValueError):
            plot_player_comparison(
                data,
                players=['Josh Allen'],
                metrics=['invalid_metric']
            )
    
    def test_create_radar_chart(self):
        """Test radar chart creation."""
        data = self.setUp_sample_player_data()
        players = ['Josh Allen', 'Lamar Jackson']
        metrics = ['passing_yards', 'passing_tds', 'qbr']
        
        fig = _create_radar_chart(
            data, players, metrics,
            'player_display_name', 'recent_team', True
        )
        
        assert isinstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_create_player_bar_chart(self):
        """Test player bar chart creation."""
        data = self.setUp_sample_player_data()
        players = ['Josh Allen', 'Tom Brady']
        metrics = ['passing_yards', 'passing_tds']
        
        fig = _create_player_bar_chart(
            data, players, metrics,
            'player_display_name', 'recent_team', True
        )
        
        assert isinstance(fig, plt.Figure)
        plt.close(fig)


class TestPlottingWithRealData:
    """Test plotting functions with more realistic NFL data."""
    
    def test_team_stats_with_all_teams(self):
        """Test with data for all 32 teams."""
        # Create realistic team data
        np.random.seed(42)
        teams = [
            'ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE',
            'DAL', 'DEN', 'DET', 'GB', 'HOU', 'IND', 'JAC', 'KC',
            'LV', 'LAC', 'LAR', 'MIA', 'MIN', 'NE', 'NO', 'NYG',
            'NYJ', 'PHI', 'PIT', 'SEA', 'SF', 'TB', 'TEN', 'WAS'
        ]
        
        data = pd.DataFrame({
            'team': teams,
            'epa_per_play': np.random.normal(0, 0.1, len(teams)),
            'success_rate': np.random.normal(0.45, 0.05, len(teams)),
            'points_per_game': np.random.normal(22, 5, len(teams))
        })
        
        fig = plot_team_stats(
            data,
            x='epa_per_play',
            y='success_rate', 
            show_logos=False,  # Avoid logo loading in tests
            add_reference_lines=True,
            reference_type='both'
        )
        
        assert isinstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_player_comparison_multiple_metrics(self):
        """Test player comparison with many metrics."""
        players_data = pd.DataFrame({
            'player_display_name': [
                'Josh Allen', 'Patrick Mahomes', 'Tom Brady', 
                'Aaron Rodgers', 'Lamar Jackson'
            ],
            'recent_team': ['BUF', 'KC', 'TB', 'GB', 'BAL'],
            'passing_yards': [4544, 4839, 4633, 4115, 3678],
            'passing_tds': [37, 37, 40, 37, 26],
            'interceptions': [15, 13, 12, 4, 9],
            'qbr': [70.3, 83.8, 78.1, 95.1, 64.2],
            'completion_percentage': [63.3, 66.3, 65.7, 70.7, 64.4],
            'yards_per_attempt': [7.4, 7.3, 8.9, 8.4, 7.3]
        })
        
        # Test radar chart with multiple metrics
        fig = plot_player_comparison(
            players_data,
            players=['Josh Allen', 'Patrick Mahomes', 'Tom Brady'],
            metrics=['passing_yards', 'passing_tds', 'qbr', 'completion_percentage'],
            plot_type='radar'
        )
        
        assert isinstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_reference_lines_combinations(self):
        """Test different reference line combinations."""
        data = pd.DataFrame({
            'team': ['ARI', 'ATL', 'BAL', 'BUF'],
            'x_metric': [1.0, 2.0, 3.0, 4.0],
            'y_metric': [2.0, 1.0, 4.0, 3.0]
        })
        
        # Test median only
        fig1 = plot_team_stats(
            data, 'x_metric', 'y_metric',
            reference_type='median', show_logos=False
        )
        assert isinstance(fig1, plt.Figure)
        plt.close(fig1)
        
        # Test mean only
        fig2 = plot_team_stats(
            data, 'x_metric', 'y_metric',
            reference_type='mean', show_logos=False
        )
        assert isinstance(fig2, plt.Figure)
        plt.close(fig2)
        
        # Test both
        fig3 = plot_team_stats(
            data, 'x_metric', 'y_metric',
            reference_type='both', show_logos=False
        )
        assert isinstance(fig3, plt.Figure)
        plt.close(fig3)
    
    def test_custom_team_column(self):
        """Test with custom team column name."""
        data = pd.DataFrame({
            'team_abbr': ['ARI', 'ATL', 'BAL'],
            'offensive_epa': [0.05, -0.02, 0.08],
            'defensive_epa': [-0.03, 0.04, -0.06]
        })
        
        fig = plot_team_stats(
            data,
            x='offensive_epa',
            y='defensive_epa',
            team_column='team_abbr',
            show_logos=False
        )
        
        assert isinstance(fig, plt.Figure)
        plt.close(fig)


class TestPlottingEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_empty_dataframe(self):
        """Test with empty DataFrame."""
        data = pd.DataFrame(columns=['team', 'x', 'y'])
        
        # Should handle gracefully
        with pytest.raises((ValueError, IndexError)):
            plot_team_stats(data, 'x', 'y', show_logos=False)
    
    def test_single_team(self):
        """Test with single team."""
        data = pd.DataFrame({
            'team': ['ARI'],
            'x': [1.0],
            'y': [2.0]
        })
        
        fig = plot_team_stats(data, 'x', 'y', show_logos=False)
        assert isinstance(fig, plt.Figure)
        plt.close(fig)
    
    def test_missing_team_column(self):
        """Test with missing team column."""
        data = pd.DataFrame({
            'x': [1, 2, 3],
            'y': [2, 1, 3]
        })
        
        with pytest.raises(ValueError):
            plot_team_stats(data, 'x', 'y')
    
    def test_invalid_team_abbreviations(self):
        """Test with invalid team abbreviations."""
        data = pd.DataFrame({
            'team': ['INVALID', 'ALSO_INVALID'],
            'x': [1, 2],
            'y': [2, 1]
        })
        
        with pytest.raises(ValueError):
            plot_team_stats(data, 'x', 'y', show_logos=False)


if __name__ == "__main__":
    pytest.main([__file__])