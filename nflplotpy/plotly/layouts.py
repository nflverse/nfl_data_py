"""Layout and styling utilities for plotly NFL visualizations."""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from typing import List, Optional, Dict, Any, Union

from ..core.colors import get_team_colors, get_palette_manager
from ..core.utils import validate_teams


def create_nfl_layout(title: Optional[str] = None,
                     teams: Optional[List[str]] = None,
                     theme: str = "default",
                     **kwargs) -> Dict[str, Any]:
    """Create plotly layout with NFL styling.
    
    Args:
        title: Plot title
        teams: Teams to base color scheme on
        theme: Theme style ('default', 'dark', 'minimal')
        **kwargs: Additional layout arguments
        
    Returns:
        Dictionary with layout configuration
    """
    if theme == "default":
        layout = _create_default_layout(title, teams, **kwargs)
    elif theme == "dark": 
        layout = _create_dark_layout(title, teams, **kwargs)
    elif theme == "minimal":
        layout = _create_minimal_layout(title, teams, **kwargs)
    else:
        raise ValueError(f"Unknown theme: {theme}")
    
    return layout


def _create_default_layout(title: Optional[str] = None,
                         teams: Optional[List[str]] = None,
                         **kwargs) -> Dict[str, Any]:
    """Create default NFL layout."""
    layout = {
        "title": {
            "text": title,
            "x": 0.5,
            "font": {"size": 18, "color": "#333333"}
        },
        "plot_bgcolor": "white",
        "paper_bgcolor": "white",
        "font": {"color": "#333333", "size": 12},
        "xaxis": {
            "showgrid": True,
            "gridcolor": "rgba(128,128,128,0.2)",
            "linecolor": "#333333",
            "linewidth": 1,
            "tickcolor": "#333333"
        },
        "yaxis": {
            "showgrid": True, 
            "gridcolor": "rgba(128,128,128,0.2)",
            "linecolor": "#333333",
            "linewidth": 1,
            "tickcolor": "#333333"
        },
        "margin": {"l": 60, "r": 60, "t": 60, "b": 60}
    }
    
    # Apply team colors if specified
    if teams is not None:
        teams = validate_teams(teams)
        if len(teams) == 1:
            primary_color = get_team_colors(teams[0], "primary")
            layout["title"]["font"]["color"] = primary_color
            layout["xaxis"]["linecolor"] = primary_color
            layout["yaxis"]["linecolor"] = primary_color
    
    # Update with custom kwargs
    layout.update(kwargs)
    return layout


def _create_dark_layout(title: Optional[str] = None,
                       teams: Optional[List[str]] = None,
                       **kwargs) -> Dict[str, Any]:
    """Create dark NFL layout."""
    layout = {
        "title": {
            "text": title,
            "x": 0.5,
            "font": {"size": 18, "color": "white"}
        },
        "plot_bgcolor": "#1e1e1e",
        "paper_bgcolor": "#2e2e2e",
        "font": {"color": "white", "size": 12},
        "xaxis": {
            "showgrid": True,
            "gridcolor": "rgba(255,255,255,0.1)",
            "linecolor": "#cccccc", 
            "linewidth": 1,
            "tickcolor": "#cccccc"
        },
        "yaxis": {
            "showgrid": True,
            "gridcolor": "rgba(255,255,255,0.1)",
            "linecolor": "#cccccc",
            "linewidth": 1,
            "tickcolor": "#cccccc"
        },
        "margin": {"l": 60, "r": 60, "t": 60, "b": 60}
    }
    
    # Apply brightened team colors if specified
    if teams is not None:
        teams = validate_teams(teams)
        if len(teams) == 1:
            primary_color = get_team_colors(teams[0], "primary")
            # Brighten color for dark theme
            import plotly.colors as pc
            rgb = pc.hex_to_rgb(primary_color)
            bright_rgb = [min(255, int(c * 1.3)) for c in rgb]
            bright_color = f"rgb({bright_rgb[0]},{bright_rgb[1]},{bright_rgb[2]})"
            
            layout["title"]["font"]["color"] = bright_color
            layout["xaxis"]["linecolor"] = bright_color
            layout["yaxis"]["linecolor"] = bright_color
    
    layout.update(kwargs)
    return layout


def _create_minimal_layout(title: Optional[str] = None,
                         teams: Optional[List[str]] = None,
                         **kwargs) -> Dict[str, Any]:
    """Create minimal NFL layout."""
    layout = {
        "title": {
            "text": title,
            "x": 0.5,
            "font": {"size": 16, "color": "#666666"}
        },
        "plot_bgcolor": "white",
        "paper_bgcolor": "white", 
        "font": {"color": "#666666", "size": 11},
        "xaxis": {
            "showgrid": False,
            "linecolor": "#cccccc",
            "linewidth": 1,
            "tickcolor": "#cccccc",
            "showline": True,
            "mirror": False
        },
        "yaxis": {
            "showgrid": False,
            "linecolor": "#cccccc", 
            "linewidth": 1,
            "tickcolor": "#cccccc",
            "showline": True,
            "mirror": False
        },
        "margin": {"l": 50, "r": 30, "t": 50, "b": 50}
    }
    
    # Apply muted team colors if specified
    if teams is not None:
        teams = validate_teams(teams)
        if len(teams) == 1:
            primary_color = get_team_colors(teams[0], "primary")
            layout["title"]["font"]["color"] = primary_color
    
    layout.update(kwargs)
    return layout


def apply_nfl_styling(fig: go.Figure, teams: Optional[List[str]] = None,
                     theme: str = "default", **kwargs) -> go.Figure:
    """Apply NFL styling to existing plotly figure.
    
    Args:
        fig: Plotly figure to style
        teams: Teams to base styling on
        theme: Theme to apply
        **kwargs: Additional layout updates
        
    Returns:
        Styled plotly figure
    """
    layout = create_nfl_layout(teams=teams, theme=theme, **kwargs)
    fig.update_layout(layout)
    return fig


def create_team_colorscale(teams: List[str], 
                          color_type: str = "primary") -> List[List]:
    """Create plotly colorscale from team colors.
    
    Args:
        teams: List of team abbreviations
        color_type: Type of color to use
        
    Returns:
        Plotly colorscale format
    """
    manager = get_palette_manager()
    return manager.to_plotly_colorscale(teams, color_type)


def create_conference_subplot(afc_data: Dict[str, Any], 
                             nfc_data: Dict[str, Any],
                             subplot_titles: Optional[List[str]] = None,
                             **kwargs) -> go.Figure:
    """Create subplot comparing AFC and NFC.
    
    Args:
        afc_data: Data for AFC subplot
        nfc_data: Data for NFC subplot  
        subplot_titles: Titles for subplots
        **kwargs: Additional arguments for make_subplots
        
    Returns:
        Plotly figure with AFC/NFC subplots
    """
    if subplot_titles is None:
        subplot_titles = ["AFC", "NFC"]
    
    # Create subplots
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=subplot_titles,
        **kwargs
    )
    
    # Add AFC data
    if "x" in afc_data and "y" in afc_data:
        fig.add_trace(
            go.Scatter(
                x=afc_data["x"],
                y=afc_data["y"],
                mode="markers",
                name="AFC",
                marker=dict(
                    size=afc_data.get("size", 10),
                    color=afc_data.get("color", "blue")
                )
            ),
            row=1, col=1
        )
    
    # Add NFC data
    if "x" in nfc_data and "y" in nfc_data:
        fig.add_trace(
            go.Scatter(
                x=nfc_data["x"],
                y=nfc_data["y"],
                mode="markers", 
                name="NFC",
                marker=dict(
                    size=nfc_data.get("size", 10),
                    color=nfc_data.get("color", "red")
                )
            ),
            row=1, col=2
        )
    
    # Apply NFL theming
    fig = apply_nfl_styling(fig, theme="default")
    
    return fig


def create_division_subplot(division_data: Dict[str, Dict[str, Any]],
                           **kwargs) -> go.Figure:
    """Create subplot for division comparisons.
    
    Args:
        division_data: Dictionary mapping division names to data
        **kwargs: Additional arguments for make_subplots
        
    Returns:
        Plotly figure with division subplots
    """
    divisions = list(division_data.keys())
    n_divisions = len(divisions)
    
    # Determine subplot layout
    if n_divisions <= 2:
        rows, cols = 1, n_divisions
    elif n_divisions <= 4:
        rows, cols = 2, 2
    else:
        rows, cols = 2, 4  # Max 8 divisions
    
    # Create subplots
    fig = make_subplots(
        rows=rows, cols=cols,
        subplot_titles=divisions,
        **kwargs
    )
    
    # Add data for each division
    for i, (division, data) in enumerate(division_data.items()):
        row = i // cols + 1
        col = i % cols + 1
        
        if "x" in data and "y" in data:
            # Get division teams for colors
            teams = data.get("teams", [])
            if teams:
                colors = get_team_colors(teams, "primary")
            else:
                colors = "blue"
            
            fig.add_trace(
                go.Scatter(
                    x=data["x"],
                    y=data["y"],
                    mode="markers",
                    name=division,
                    marker=dict(
                        size=data.get("size", 10),
                        color=colors
                    ),
                    showlegend=False
                ),
                row=row, col=col
            )
    
    # Apply NFL theming
    fig = apply_nfl_styling(fig, theme="default")
    
    return fig


def add_nfl_watermark(fig: go.Figure, text: str = "nflplotpy",
                     x: float = 0.99, y: float = 0.01,
                     opacity: float = 0.3, **kwargs) -> go.Figure:
    """Add watermark to plotly figure.
    
    Args:
        fig: Plotly figure
        text: Watermark text
        x: X position (0-1)
        y: Y position (0-1) 
        opacity: Text opacity
        **kwargs: Additional text arguments
        
    Returns:
        Figure with watermark
    """
    fig.add_annotation(
        text=text,
        xref="paper", yref="paper",
        x=x, y=y,
        showarrow=False,
        font=dict(
            size=10,
            color="gray"
        ),
        opacity=opacity,
        **kwargs
    )
    return fig