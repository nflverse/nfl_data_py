"""Custom plotly traces for NFL logos and headshots."""

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from typing import List, Union, Optional, Dict, Any
import base64
from io import BytesIO

from ..core.logos import get_team_logo, get_asset_manager
from ..core.colors import get_team_colors
from ..core.utils import validate_teams


def _pil_to_base64(pil_image) -> str:
    """Convert PIL image to base64 string for plotly."""
    buffer = BytesIO()
    pil_image.save(buffer, format='PNG')
    img_str = base64.b64encode(buffer.getvalue()).decode()
    return f"data:image/png;base64,{img_str}"


def add_nfl_logo_trace(fig: go.Figure, team: str, x: float, y: float,
                      size: float = 0.1, opacity: float = 1.0, 
                      layer: str = "above", **kwargs) -> go.Figure:
    """Add NFL team logo as image to plotly figure.
    
    Args:
        fig: Plotly figure
        team: Team abbreviation
        x: X position (in data coordinates)
        y: Y position (in data coordinates)
        size: Logo size (relative to plot)
        opacity: Logo opacity (0-1)
        layer: Image layer ('above', 'below')
        **kwargs: Additional arguments
        
    Returns:
        Updated plotly figure
    """
    # Validate team
    validate_teams(team)
    
    try:
        # Get logo image
        pil_image = get_team_logo(team)
        
        # Convert to base64
        img_base64 = _pil_to_base64(pil_image)
        
        # Add image to layout
        fig.add_layout_image(
            dict(
                source=img_base64,
                xref="x",
                yref="y", 
                x=x,
                y=y,
                sizex=size,
                sizey=size,
                sizing="contain",
                opacity=opacity,
                layer=layer,
                xanchor="center",
                yanchor="middle",
                **kwargs
            )
        )
        
        return fig
        
    except Exception as e:
        print(f"Warning: Could not add logo for {team}: {e}")
        return fig


def add_nfl_logos_trace(fig: go.Figure, teams: List[str], 
                       x: Union[List[float], np.ndarray],
                       y: Union[List[float], np.ndarray],
                       size: float = 0.1, **kwargs) -> go.Figure:
    """Add multiple NFL team logos to plotly figure.
    
    Args:
        fig: Plotly figure
        teams: List of team abbreviations
        x: X positions
        y: Y positions
        size: Logo size for all logos
        **kwargs: Additional arguments
        
    Returns:
        Updated plotly figure
        
    Raises:
        ValueError: If arrays have different lengths
    """
    if len(teams) != len(x) or len(teams) != len(y):
        raise ValueError("teams, x, and y must have the same length")
    
    for team, xi, yi in zip(teams, x, y):
        fig = add_nfl_logo_trace(fig, team, xi, yi, size=size, **kwargs)
    
    return fig


def add_nfl_headshot_trace(fig: go.Figure, player_id: str, x: float, y: float,
                          size: float = 0.1, **kwargs) -> go.Figure:
    """Add NFL player headshot to plotly figure.
    
    Args:
        fig: Plotly figure
        player_id: Player ID or name
        x: X position
        y: Y position
        size: Headshot size
        **kwargs: Additional arguments
        
    Returns:
        Updated plotly figure
    """
    try:
        # Get asset manager
        manager = get_asset_manager()
        pil_image = manager.get_headshot(player_id)
        
        # Convert to base64
        img_base64 = _pil_to_base64(pil_image)
        
        # Add image to layout
        fig.add_layout_image(
            dict(
                source=img_base64,
                xref="x",
                yref="y",
                x=x,
                y=y,
                sizex=size,
                sizey=size,
                sizing="contain",
                opacity=1.0,
                layer="above",
                xanchor="center",
                yanchor="middle",
                **kwargs
            )
        )
        
        return fig
        
    except Exception as e:
        print(f"Warning: Could not add headshot for {player_id}: {e}")
        return fig


def create_team_scatter(teams: List[str], x: List[float], y: List[float],
                       color_type: str = "primary", 
                       show_logos: bool = True,
                       logo_size: float = 0.05,
                       marker_size: Optional[float] = None,
                       **kwargs) -> go.Figure:
    """Create scatter plot with team colors and optional logos.
    
    Args:
        teams: List of team abbreviations
        x: X values
        y: Y values  
        color_type: Type of team color to use
        show_logos: Whether to show team logos
        logo_size: Size of logos
        marker_size: Size of markers (if not showing logos)
        **kwargs: Additional arguments passed to go.Scatter
        
    Returns:
        Plotly figure with scatter plot
    """
    # Validate inputs
    if len(teams) != len(x) or len(teams) != len(y):
        raise ValueError("teams, x, and y must have the same length")
    
    teams = validate_teams(teams)
    
    # Get team colors
    colors = get_team_colors(teams, color_type)
    
    # Create figure
    fig = go.Figure()
    
    if show_logos:
        # Create invisible scatter trace for hover info
        fig.add_trace(go.Scatter(
            x=x,
            y=y,
            mode='markers',
            marker=dict(
                size=20,
                color='rgba(0,0,0,0)',  # Transparent
                line=dict(width=0)
            ),
            text=teams,
            hovertemplate='<b>%{text}</b><br>X: %{x}<br>Y: %{y}<extra></extra>',
            showlegend=False,
            **kwargs
        ))
        
        # Add logos
        fig = add_nfl_logos_trace(fig, teams, x, y, size=logo_size)
        
    else:
        # Create scatter with team colors
        marker_size = marker_size or 15
        
        fig.add_trace(go.Scatter(
            x=x,
            y=y,
            mode='markers',
            marker=dict(
                size=marker_size,
                color=colors,
                line=dict(width=2, color='white')
            ),
            text=teams,
            hovertemplate='<b>%{text}</b><br>X: %{x}<br>Y: %{y}<extra></extra>',
            showlegend=False,
            **kwargs
        ))
    
    return fig


def create_team_bar(teams: List[str], values: List[float],
                   color_type: str = "primary",
                   orientation: str = "v",
                   show_logos: bool = False,
                   **kwargs) -> go.Figure:
    """Create bar chart with team colors.
    
    Args:
        teams: List of team abbreviations
        values: Bar values
        color_type: Type of team color to use
        orientation: Bar orientation ('v' for vertical, 'h' for horizontal)
        show_logos: Whether to add team logos (experimental)
        **kwargs: Additional arguments passed to go.Bar
        
    Returns:
        Plotly figure with bar chart
    """
    # Validate inputs
    if len(teams) != len(values):
        raise ValueError("teams and values must have the same length")
    
    teams = validate_teams(teams)
    
    # Get team colors
    colors = get_team_colors(teams, color_type)
    
    # Create figure
    fig = go.Figure()
    
    if orientation == "v":
        fig.add_trace(go.Bar(
            x=teams,
            y=values,
            marker_color=colors,
            text=teams,
            textposition='auto',
            **kwargs
        ))
    else:  # horizontal
        fig.add_trace(go.Bar(
            x=values,
            y=teams,
            orientation='h',
            marker_color=colors,
            text=teams,
            textposition='auto',
            **kwargs
        ))
    
    return fig


def add_median_lines(fig: go.Figure, data: List[float], 
                    axis: str = 'both', **kwargs) -> go.Figure:
    """Add median reference lines to plotly figure.
    
    Args:
        fig: Plotly figure
        data: Data to calculate median from
        axis: Which axis to add lines ('x', 'y', 'both')
        **kwargs: Arguments passed to add_hline/add_vline
        
    Returns:
        Updated plotly figure
    """
    median_val = np.median(data)
    
    # Default line styling
    line_kwargs = {
        'line_color': 'red',
        'line_dash': 'dash',
        'opacity': 0.7,
        'line_width': 1
    }
    line_kwargs.update(kwargs)
    
    if axis in ['y', 'both']:
        fig.add_hline(y=median_val, **line_kwargs)
    
    if axis in ['x', 'both']:
        fig.add_vline(x=median_val, **line_kwargs)
    
    return fig


def add_mean_lines(fig: go.Figure, data: List[float], 
                  axis: str = 'both', **kwargs) -> go.Figure:
    """Add mean reference lines to plotly figure.
    
    Args:
        fig: Plotly figure
        data: Data to calculate mean from
        axis: Which axis to add lines ('x', 'y', 'both')
        **kwargs: Arguments passed to add_hline/add_vline
        
    Returns:
        Updated plotly figure
    """
    mean_val = np.mean(data)
    
    # Default line styling
    line_kwargs = {
        'line_color': 'blue',
        'line_dash': 'solid',
        'opacity': 0.7,
        'line_width': 1
    }
    line_kwargs.update(kwargs)
    
    if axis in ['y', 'both']:
        fig.add_hline(y=mean_val, **line_kwargs)
    
    if axis in ['x', 'both']:
        fig.add_vline(x=mean_val, **line_kwargs)
    
    return fig