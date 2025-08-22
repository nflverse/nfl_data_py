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
from .core.utils import (
    team_factor, team_tiers, get_nflverse_info, validate_teams,
    nfl_sitrep, clear_all_cache, validate_player_ids
)

# Enhanced URL management
from .core.urls import (
    get_team_wordmark_url, get_player_headshot_urls, 
    discover_player_id, AssetURLManager
)

# Matplotlib integration
from .matplotlib.artists import add_nfl_logo, add_nfl_headshot, add_nfl_wordmark, add_median_lines, add_mean_lines
from .matplotlib.scales import nfl_color_scale, apply_nfl_theme
from .matplotlib.preview import nfl_preview, preview_with_dimensions, ggpreview
from .matplotlib.elements import (
    set_xlabel_with_logos, set_ylabel_with_logos, set_title_with_logos,
    add_logo_watermark, create_team_comparison_axes
)

# Pandas integration
from .pandas.styling import (
    style_with_logos, style_with_headshots, style_with_wordmarks,
    create_nfl_table, NFLTableStyler
)

# High-level plotting functions
from .core.plotting import plot_team_stats, plot_player_comparison

__all__ = [
    # Core classes
    "NFLAssetManager",
    "NFLColorPalette", 
    "NFL_TEAM_COLORS",
    "AssetURLManager",
    "NFLTableStyler",
    
    # Utility functions
    "team_factor",
    "team_tiers",
    "get_available_teams",
    "get_team_colors",
    "get_nflverse_info",
    "validate_teams",
    "nfl_sitrep",
    "clear_all_cache",
    "validate_player_ids",
    
    # URL management
    "get_team_wordmark_url",
    "get_player_headshot_urls",
    "discover_player_id",
    
    # Matplotlib functions
    "add_nfl_logo",
    "add_nfl_headshot", 
    "add_nfl_wordmark",
    "add_median_lines",
    "add_mean_lines",
    "nfl_color_scale",
    "apply_nfl_theme",
    
    # Matplotlib preview and elements
    "nfl_preview",
    "preview_with_dimensions", 
    "ggpreview",
    "set_xlabel_with_logos",
    "set_ylabel_with_logos",
    "set_title_with_logos",
    "add_logo_watermark",
    "create_team_comparison_axes",
    
    # Pandas integration
    "style_with_logos",
    "style_with_headshots",
    "style_with_wordmarks",
    "create_nfl_table",
    
    # High-level plotting
    "plot_team_stats",
    "plot_player_comparison",
]