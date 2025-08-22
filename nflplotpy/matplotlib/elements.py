"""Theme elements for integrating logos into plot components.

Provides functions to replace text elements (axis labels, titles, etc.)
with NFL team logos, similar to R's element_nfl_logo() functionality.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from matplotlib.patches import Rectangle
from matplotlib.text import Text
import numpy as np
from typing import List, Optional, Union, Dict, Any, Tuple
from PIL import Image
import warnings

from ..core.logos import get_team_logo, get_available_teams
from ..core.colors import get_team_colors
from ..core.utils import validate_teams
from .artists import add_nfl_logo


def set_xlabel_with_logos(ax: plt.Axes, teams: List[str], 
                         positions: Optional[List[float]] = None,
                         logo_size: float = 0.05,
                         text_labels: bool = False,
                         **kwargs):
    """Replace x-axis labels with team logos.
    
    Equivalent to R's element_nfl_logo() for x-axis.
    
    Args:
        ax: matplotlib Axes
        teams: List of team abbreviations
        positions: X positions for logos. If None, uses evenly spaced positions.
        logo_size: Size of logos
        text_labels: Whether to keep text labels alongside logos
        **kwargs: Additional arguments for logo placement
    """
    teams = validate_teams(teams)
    
    if positions is None:
        positions = np.linspace(0, len(teams)-1, len(teams))
    
    if len(positions) != len(teams):
        raise ValueError("Length of positions must match length of teams")
    
    # Clear existing x-axis labels
    if not text_labels:
        ax.set_xticklabels([])
    
    # Set x-axis ticks at positions
    ax.set_xticks(positions)
    
    # Add team logos below x-axis
    y_offset = -0.1  # Position below axis
    
    for team, pos in zip(teams, positions):
        try:
            # Add logo
            add_nfl_logo(
                ax, team, pos, y_offset,
                width=logo_size,
                transform=ax.transData,
                clip_on=False,
                **kwargs
            )
        except Exception as e:
            warnings.warn(f"Could not add logo for {team}: {e}")
    
    # Adjust bottom margin to accommodate logos
    plt.subplots_adjust(bottom=0.2)


def set_ylabel_with_logos(ax: plt.Axes, teams: List[str],
                         positions: Optional[List[float]] = None,
                         logo_size: float = 0.05,
                         text_labels: bool = False,
                         **kwargs):
    """Replace y-axis labels with team logos.
    
    Equivalent to R's element_nfl_logo() for y-axis.
    
    Args:
        ax: matplotlib Axes
        teams: List of team abbreviations
        positions: Y positions for logos. If None, uses evenly spaced positions.
        logo_size: Size of logos
        text_labels: Whether to keep text labels alongside logos
        **kwargs: Additional arguments for logo placement
    """
    teams = validate_teams(teams)
    
    if positions is None:
        positions = np.linspace(0, len(teams)-1, len(teams))
    
    if len(positions) != len(teams):
        raise ValueError("Length of positions must match length of teams")
    
    # Clear existing y-axis labels
    if not text_labels:
        ax.set_yticklabels([])
    
    # Set y-axis ticks at positions
    ax.set_yticks(positions)
    
    # Add team logos to the left of y-axis
    x_offset = -0.1  # Position to left of axis
    
    for team, pos in zip(teams, positions):
        try:
            # Add logo
            add_nfl_logo(
                ax, team, x_offset, pos,
                width=logo_size,
                transform=ax.transData,
                clip_on=False,
                **kwargs
            )
        except Exception as e:
            warnings.warn(f"Could not add logo for {team}: {e}")
    
    # Adjust left margin to accommodate logos
    plt.subplots_adjust(left=0.2)


def set_title_with_logos(ax: plt.Axes, title_text: str, teams: List[str],
                        logo_positions: str = "sides",
                        logo_size: float = 0.08,
                        title_fontsize: int = 14,
                        **kwargs):
    """Add team logos to plot title.
    
    Args:
        ax: matplotlib Axes
        title_text: Main title text
        teams: List of team abbreviations (max 2 for sides positioning)
        logo_positions: Where to place logos ('sides', 'above', 'below')
        logo_size: Size of logos
        title_fontsize: Font size for title text
        **kwargs: Additional arguments
    """
    teams = validate_teams(teams)
    
    # Set main title
    title = ax.set_title(title_text, fontsize=title_fontsize, pad=20)
    
    if logo_positions == "sides" and len(teams) <= 2:
        # Place logos on left and right sides of title
        title_bbox = title.get_window_extent()
        fig = ax.get_figure()
        
        # Convert to axes coordinates
        title_center = title_bbox.x0 + title_bbox.width / 2
        title_y = title_bbox.y1 + 10
        
        if len(teams) >= 1:
            # Left logo
            add_nfl_logo(
                ax, teams[0],
                title_center - title_bbox.width/2 - 50, title_y,
                width=logo_size,
                transform=fig.transFigure,
                **kwargs
            )
        
        if len(teams) == 2:
            # Right logo
            add_nfl_logo(
                ax, teams[1], 
                title_center + title_bbox.width/2 + 50, title_y,
                width=logo_size,
                transform=fig.transFigure,
                **kwargs
            )
    
    elif logo_positions == "above":
        # Place logos above title in a row
        fig = ax.get_figure()
        
        # Calculate positions
        n_teams = len(teams)
        if n_teams > 6:
            warnings.warn("Too many teams for above positioning, using first 6")
            teams = teams[:6]
            n_teams = 6
        
        # Center the logos above the plot
        start_x = 0.5 - (n_teams * 0.1) / 2
        y_pos = 0.95  # Near top of figure
        
        for i, team in enumerate(teams):
            x_pos = start_x + i * 0.1
            add_nfl_logo(
                ax, team, x_pos, y_pos,
                width=logo_size,
                transform=fig.transFigure,
                **kwargs
            )
    
    else:
        warnings.warn(f"Unsupported logo_positions: {logo_positions}")


def add_logo_watermark(ax: plt.Axes, team: str,
                      position: str = "bottom_right",
                      alpha: float = 0.3,
                      logo_size: float = 0.15):
    """Add team logo as watermark to plot.
    
    Args:
        ax: matplotlib Axes
        team: Team abbreviation
        position: Where to place watermark ('bottom_right', 'bottom_left', 
                 'top_right', 'top_left', 'center')
        alpha: Transparency level
        logo_size: Size of watermark logo
    """
    team = validate_teams(team)[0]
    
    # Position mapping
    positions = {
        'bottom_right': (0.85, 0.15),
        'bottom_left': (0.15, 0.15),
        'top_right': (0.85, 0.85),
        'top_left': (0.15, 0.85),
        'center': (0.5, 0.5)
    }
    
    if position not in positions:
        raise ValueError(f"Invalid position: {position}. "
                        f"Choose from: {list(positions.keys())}")
    
    x, y = positions[position]
    
    # Add logo with transparency
    add_nfl_logo(
        ax, team, x, y,
        width=logo_size,
        alpha=alpha,
        transform=ax.transAxes,
        zorder=0  # Behind other elements
    )


def create_team_comparison_axes(fig: plt.Figure, team1: str, team2: str,
                               title: Optional[str] = None) -> Tuple[plt.Axes, plt.Axes]:
    """Create split axes for team vs team comparisons.
    
    Args:
        fig: matplotlib Figure
        team1: First team abbreviation
        team2: Second team abbreviation
        title: Optional main title
        
    Returns:
        Tuple of (left_ax, right_ax) for each team
    """
    teams = validate_teams([team1, team2])
    team1, team2 = teams
    
    # Create two side-by-side subplots
    left_ax = fig.add_subplot(121)
    right_ax = fig.add_subplot(122)
    
    # Get team colors
    color1 = get_team_colors(team1, "primary")
    color2 = get_team_colors(team2, "primary") 
    
    # Style each subplot with team colors
    from .scales import apply_nfl_theme
    apply_nfl_theme(left_ax, team=team1)
    apply_nfl_theme(right_ax, team=team2)
    
    # Add team logos as subplot titles
    add_nfl_logo(left_ax, team1, 0.5, 1.1, width=0.1, 
                transform=left_ax.transAxes)
    add_nfl_logo(right_ax, team2, 0.5, 1.1, width=0.1,
                transform=right_ax.transAxes)
    
    # Main title if provided
    if title:
        fig.suptitle(title, fontsize=16, fontweight='bold')
    
    return left_ax, right_ax


def add_conference_logos(ax: plt.Axes, position: str = "corners",
                        logo_size: float = 0.08, alpha: float = 0.6):
    """Add AFC and NFC logos to plot.
    
    Args:
        ax: matplotlib Axes
        position: Where to place logos ('corners', 'top', 'bottom')  
        logo_size: Size of logos
        alpha: Transparency level
    """
    if position == "corners":
        # AFC in top-left, NFC in top-right
        add_nfl_logo(ax, 'AFC', 0.05, 0.95, width=logo_size, 
                    alpha=alpha, transform=ax.transAxes)
        add_nfl_logo(ax, 'NFC', 0.95, 0.95, width=logo_size,
                    alpha=alpha, transform=ax.transAxes)
    
    elif position == "top":
        # Both at top, centered
        add_nfl_logo(ax, 'AFC', 0.4, 0.95, width=logo_size,
                    alpha=alpha, transform=ax.transAxes)
        add_nfl_logo(ax, 'NFC', 0.6, 0.95, width=logo_size, 
                    alpha=alpha, transform=ax.transAxes)
    
    elif position == "bottom":
        # Both at bottom, centered
        add_nfl_logo(ax, 'AFC', 0.4, 0.05, width=logo_size,
                    alpha=alpha, transform=ax.transAxes)
        add_nfl_logo(ax, 'NFC', 0.6, 0.05, width=logo_size,
                    alpha=alpha, transform=ax.transAxes)
    
    else:
        raise ValueError(f"Invalid position: {position}")


def create_division_subplot_grid(fig: plt.Figure, division_data: Dict[str, List[str]],
                               title: Optional[str] = None) -> Dict[str, plt.Axes]:
    """Create subplot grid organized by NFL divisions.
    
    Args:
        fig: matplotlib Figure  
        division_data: Dictionary mapping division names to team lists
        title: Optional main title
        
    Returns:
        Dictionary mapping division names to their axes
    """
    n_divisions = len(division_data)
    
    # Determine grid layout
    if n_divisions <= 4:
        rows, cols = 2, 2
    elif n_divisions <= 6:
        rows, cols = 2, 3
    elif n_divisions <= 8:
        rows, cols = 2, 4
    else:
        rows = int(np.ceil(n_divisions / 4))
        cols = 4
    
    axes = {}
    
    for i, (division, teams) in enumerate(division_data.items()):
        if i >= rows * cols:
            break
            
        ax = fig.add_subplot(rows, cols, i + 1)
        axes[division] = ax
        
        # Add division title with team logos
        ax.set_title(division, fontsize=12, fontweight='bold', pad=20)
        
        # Add small team logos above subplot
        if teams:
            n_teams = min(len(teams), 4)  # Max 4 logos per division
            for j, team in enumerate(teams[:n_teams]):
                x_pos = 0.2 + j * 0.15
                try:
                    add_nfl_logo(ax, team, x_pos, 1.05, width=0.06,
                               transform=ax.transAxes)
                except Exception:
                    pass  # Skip invalid teams
    
    if title:
        fig.suptitle(title, fontsize=16, fontweight='bold', y=0.98)
    
    return axes