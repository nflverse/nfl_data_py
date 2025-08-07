"""
nflplotpy - Python NFL Visualization Package

A Python equivalent to R's nflplotR package, providing NFL-specific
plotting capabilities with matplotlib, plotly, and seaborn integration.
"""

__version__ = "0.1.0"
__author__ = "nflverse"

# Core functionality
from .core.logos import NFLAssetManager, get_available_teams
from .core.colors import NFLColorPalette, NFL_TEAM_COLORS, get_team_colors
from .core.utils import team_factor, team_tiers, get_nflverse_info, validate_teams

# Matplotlib integration
from .matplotlib.artists import add_nfl_logo, add_nfl_headshot, add_nfl_wordmark, add_median_lines, add_mean_lines
from .matplotlib.scales import nfl_color_scale, apply_nfl_theme

# High-level plotting functions
from .core.plotting import plot_team_stats, plot_player_comparison

__all__ = [
    # Core classes
    "NFLAssetManager",
    "NFLColorPalette", 
    "NFL_TEAM_COLORS",
    
    # Utility functions
    "team_factor",
    "team_tiers",
    "get_available_teams",
    "get_team_colors",
    "get_nflverse_info",
    "validate_teams",
    
    # Matplotlib functions
    "add_nfl_logo",
    "add_nfl_headshot", 
    "add_nfl_wordmark",
    "add_median_lines",
    "add_mean_lines",
    "nfl_color_scale",
    "apply_nfl_theme",
    
    # High-level plotting
    "plot_team_stats",
    "plot_player_comparison",
]