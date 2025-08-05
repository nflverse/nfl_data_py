"""Color scales and themes for matplotlib integration."""

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.axes import Axes
from typing import List, Optional, Union, Dict, Any

from ..core.colors import get_team_colors, create_nfl_colormap, get_palette_manager
from ..core.utils import validate_teams


def nfl_color_scale(teams: List[str], color_type: str = "primary") -> mcolors.ListedColormap:
    """Create matplotlib colormap from NFL team colors.
    
    Equivalent to nflplotR's scale_color_nfl().
    
    Args:
        teams: List of team abbreviations
        color_type: Type of color ('primary', 'secondary', 'tertiary', 'quaternary')
        
    Returns:
        matplotlib ListedColormap
    """
    return create_nfl_colormap(teams, color_type)


def set_team_colors(ax: Axes, teams: List[str], data_values: Optional[List[Any]] = None,
                   color_type: str = "primary", **kwargs):
    """Set team colors for plot elements.
    
    Args:
        ax: Matplotlib axes
        teams: List of team abbreviations
        data_values: Values to map colors to (if None, uses teams directly)
        color_type: Type of color to use
        **kwargs: Additional arguments
    """
    # Validate teams
    teams = validate_teams(teams)
    
    # Get colors
    colors = get_team_colors(teams, color_type)
    
    # Create color mapping
    if data_values is None:
        data_values = teams
    
    color_map = dict(zip(data_values, colors))
    
    return color_map


def set_team_fill_colors(ax: Axes, teams: List[str], 
                        color_type: str = "primary", **kwargs):
    """Set team fill colors for plot elements.
    
    Equivalent to nflplotR's scale_fill_nfl().
    
    Args:
        ax: Matplotlib axes  
        teams: List of team abbreviations
        color_type: Type of color to use
        **kwargs: Additional arguments
    """
    return set_team_colors(ax, teams, color_type=color_type, **kwargs)


def apply_nfl_theme(ax: Axes, team: Optional[str] = None, 
                   style: str = "default", **kwargs):
    """Apply NFL-themed styling to matplotlib axes.
    
    Args:
        ax: Matplotlib axes
        team: Specific team to theme around (optional)
        style: Theme style ('default', 'minimal', 'dark')
        **kwargs: Additional styling arguments
    """
    if style == "default":
        _apply_default_nfl_theme(ax, team, **kwargs)
    elif style == "minimal":
        _apply_minimal_nfl_theme(ax, team, **kwargs)
    elif style == "dark":
        _apply_dark_nfl_theme(ax, team, **kwargs)
    else:
        raise ValueError(f"Unknown style: {style}")


def _apply_default_nfl_theme(ax: Axes, team: Optional[str] = None, **kwargs):
    """Apply default NFL theme styling."""
    
    # Grid styling
    ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
    ax.set_axisbelow(True)
    
    # Spine styling
    for spine in ax.spines.values():
        spine.set_linewidth(1.5)
        spine.set_color('#333333')
    
    # Tick styling
    ax.tick_params(
        which='major',
        labelsize=10,
        color='#333333',
        width=1,
        length=5
    )
    
    # If team is specified, use team colors
    if team is not None:
        team = validate_teams(team)[0]
        primary_color = get_team_colors(team, "primary")
        secondary_color = get_team_colors(team, "secondary")
        
        # Apply team colors to spines
        ax.spines['top'].set_color(primary_color)
        ax.spines['bottom'].set_color(primary_color)
        ax.spines['left'].set_color(primary_color)
        ax.spines['right'].set_color(primary_color)
        
        # Apply to ticks
        ax.tick_params(color=primary_color)


def _apply_minimal_nfl_theme(ax: Axes, team: Optional[str] = None, **kwargs):
    """Apply minimal NFL theme styling."""
    
    # Remove top and right spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Style remaining spines
    ax.spines['left'].set_linewidth(1)
    ax.spines['bottom'].set_linewidth(1)
    ax.spines['left'].set_color('#666666')
    ax.spines['bottom'].set_color('#666666')
    
    # Minimal grid
    ax.grid(True, alpha=0.2, linestyle='-', linewidth=0.5)
    ax.set_axisbelow(True)
    
    # Clean tick styling
    ax.tick_params(
        which='major',
        labelsize=9,
        color='#666666',
        width=0.5,
        length=3
    )
    
    if team is not None:
        team = validate_teams(team)[0]
        primary_color = get_team_colors(team, "primary")
        
        ax.spines['left'].set_color(primary_color)
        ax.spines['bottom'].set_color(primary_color)


def _apply_dark_nfl_theme(ax: Axes, team: Optional[str] = None, **kwargs):
    """Apply dark NFL theme styling."""
    
    # Dark background
    ax.set_facecolor('#1e1e1e')
    
    # Light grid
    ax.grid(True, alpha=0.2, linestyle='-', linewidth=0.5, color='white')
    ax.set_axisbelow(True)
    
    # Light spines
    for spine in ax.spines.values():
        spine.set_linewidth(1.5)
        spine.set_color('#cccccc')
    
    # Light ticks and labels
    ax.tick_params(
        which='major',
        labelsize=10,
        color='#cccccc',
        labelcolor='white',
        width=1,
        length=5
    )
    
    if team is not None:
        team = validate_teams(team)[0]
        primary_color = get_team_colors(team, "primary")
        
        # Brighten team color for dark theme
        import matplotlib.colors as mcolors
        rgb = mcolors.hex2color(primary_color)
        # Simple brightness adjustment
        bright_rgb = tuple(min(1.0, c * 1.3) for c in rgb)
        bright_color = mcolors.rgb2hex(bright_rgb)
        
        for spine in ax.spines.values():
            spine.set_color(bright_color)


def create_team_scatter_colors(teams: List[str], values: Optional[List[float]] = None,
                              color_type: str = "primary", 
                              colormap: Optional[str] = None) -> List[str]:
    """Create colors for scatter plot points based on teams.
    
    Args:
        teams: List of team abbreviations
        values: Optional values to map to colors (for gradient coloring)
        color_type: Type of team color to use
        colormap: Optional matplotlib colormap name
        
    Returns:
        List of color values for each point
    """
    if values is None:
        # Use team colors directly
        return get_team_colors(teams, color_type)
    else:
        # Use colormap with team color influence
        if colormap is None:
            # Create custom colormap from team colors
            unique_teams = list(set(teams))
            cmap = nfl_color_scale(unique_teams, color_type)
        else:
            cmap = plt.get_cmap(colormap)
        
        # Normalize values
        norm = mcolors.Normalize(vmin=min(values), vmax=max(values))
        colors = [cmap(norm(v)) for v in values]
        
        return colors


def add_team_color_legend(ax: Axes, teams: List[str], 
                         color_type: str = "primary",
                         loc: str = "best", **kwargs):
    """Add legend showing team colors.
    
    Args:
        ax: Matplotlib axes
        teams: List of team abbreviations to include in legend
        color_type: Type of color to show
        loc: Legend location
        **kwargs: Additional arguments passed to legend()
        
    Returns:
        matplotlib Legend object
    """
    from matplotlib.patches import Patch
    
    # Validate teams
    teams = validate_teams(teams)
    
    # Get colors
    colors = get_team_colors(teams, color_type)
    
    # Create legend elements
    legend_elements = [
        Patch(facecolor=color, label=team)
        for team, color in zip(teams, colors)
    ]
    
    # Default legend styling
    legend_kwargs = {
        'frameon': True,
        'fancybox': True,
        'shadow': True,
        'framealpha': 0.9,
        'fontsize': 9
    }
    legend_kwargs.update(kwargs)
    
    return ax.legend(handles=legend_elements, loc=loc, **legend_kwargs)