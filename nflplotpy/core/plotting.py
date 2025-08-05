"""High-level plotting functions with nfl_data_py integration."""

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from typing import List, Optional, Union, Dict, Any, Tuple
import warnings

from .colors import get_team_colors
from .utils import validate_teams, clean_team_abbreviations
from ..matplotlib.artists import add_nfl_logos, add_median_lines, add_mean_lines
from ..matplotlib.scales import apply_nfl_theme, set_team_colors


def plot_team_stats(data: pd.DataFrame, x: str, y: str, 
                   team_column: str = 'team',
                   backend: str = "matplotlib",
                   show_logos: bool = True,
                   logo_size: float = 0.1,
                   add_reference_lines: bool = True,
                   reference_type: str = "median",
                   figsize: Tuple[float, float] = (12, 8),
                   title: Optional[str] = None,
                   **kwargs) -> Union[plt.Figure, Any]:
    """Create team-based statistical plots with logos.
    
    Args:
        data: DataFrame with team statistics
        x: Column name for x-axis
        y: Column name for y-axis  
        team_column: Column name containing team abbreviations
        backend: Plotting backend ('matplotlib', 'plotly')
        show_logos: Whether to show team logos
        logo_size: Size of team logos
        add_reference_lines: Whether to add reference lines
        reference_type: Type of reference lines ('median', 'mean', 'both')
        figsize: Figure size for matplotlib
        title: Plot title
        **kwargs: Additional plotting arguments
        
    Returns:
        matplotlib Figure or plotly Figure depending on backend
        
    Raises:
        ValueError: If required columns are missing or invalid
    """
    # Validate inputs
    required_cols = [x, y, team_column]
    missing_cols = [col for col in required_cols if col not in data.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")
    
    # Clean team abbreviations
    df = clean_team_abbreviations(data, team_column)
    
    # Validate teams
    teams = df[team_column].tolist()
    teams = validate_teams(teams, allow_conferences=False)
    
    if backend == "matplotlib":
        return _plot_team_stats_matplotlib(
            df, x, y, team_column, show_logos, logo_size,
            add_reference_lines, reference_type, figsize, title, **kwargs
        )
    elif backend == "plotly":
        return _plot_team_stats_plotly(
            df, x, y, team_column, show_logos, logo_size,
            add_reference_lines, reference_type, title, **kwargs
        )
    else:
        raise ValueError(f"Unsupported backend: {backend}")


def _plot_team_stats_matplotlib(data: pd.DataFrame, x: str, y: str,
                               team_column: str, show_logos: bool, logo_size: float,
                               add_reference_lines: bool, reference_type: str,
                               figsize: Tuple[float, float], title: Optional[str],
                               **kwargs) -> plt.Figure:
    """Create matplotlib team stats plot."""
    fig, ax = plt.subplots(figsize=figsize)
    
    # Get team colors
    teams = data[team_column].tolist()
    colors = get_team_colors(teams, "primary")
    
    if show_logos:
        # Create scatter plot with transparent markers for positioning
        scatter = ax.scatter(
            data[x], data[y],
            c='white', s=1, alpha=0.01,  # Nearly invisible
            **kwargs
        )
        
        # Add logos
        add_nfl_logos(
            ax, teams, data[x].values, data[y].values, 
            width=logo_size
        )
    else:
        # Regular scatter plot with team colors
        scatter = ax.scatter(
            data[x], data[y],
            c=colors, s=100, alpha=0.8,
            edgecolors='white', linewidth=2,
            **kwargs
        )
    
    # Add reference lines
    if add_reference_lines:
        if reference_type in ['median', 'both']:
            add_median_lines(ax, data[x].values, axis='x', alpha=0.5)
            add_median_lines(ax, data[y].values, axis='y', alpha=0.5)
        if reference_type in ['mean', 'both']:
            add_mean_lines(ax, data[x].values, axis='x', alpha=0.5)
            add_mean_lines(ax, data[y].values, axis='y', alpha=0.5)
    
    # Styling
    ax.set_xlabel(x.replace('_', ' ').title(), fontsize=12)
    ax.set_ylabel(y.replace('_', ' ').title(), fontsize=12)
    
    if title:
        ax.set_title(title, fontsize=16, fontweight='bold')
    
    # Apply NFL theme
    apply_nfl_theme(ax, style="default")
    
    # Add grid
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig


def _plot_team_stats_plotly(data: pd.DataFrame, x: str, y: str,
                           team_column: str, show_logos: bool, logo_size: float,
                           add_reference_lines: bool, reference_type: str,
                           title: Optional[str], **kwargs):
    """Create plotly team stats plot."""
    try:
        from ..plotly.traces import create_team_scatter, add_median_lines, add_mean_lines
        from ..plotly.layouts import apply_nfl_styling
    except ImportError:
        raise ImportError("Plotly backend requires plotly to be installed")
    
    # Create scatter plot
    fig = create_team_scatter(
        teams=data[team_column].tolist(),
        x=data[x].tolist(),
        y=data[y].tolist(),
        show_logos=show_logos,
        logo_size=logo_size,
        **kwargs
    )
    
    # Add reference lines
    if add_reference_lines:
        if reference_type in ['median', 'both']:
            fig = add_median_lines(fig, data[x].values, axis='x')
            fig = add_median_lines(fig, data[y].values, axis='y')
        if reference_type in ['mean', 'both']:
            fig = add_mean_lines(fig, data[x].values, axis='x')
            fig = add_mean_lines(fig, data[y].values, axis='y')
    
    # Update layout
    fig.update_layout(
        xaxis_title=x.replace('_', ' ').title(),
        yaxis_title=y.replace('_', ' ').title(),
        title=title
    )
    
    # Apply NFL styling
    fig = apply_nfl_styling(fig, theme="default")
    
    return fig


def plot_player_comparison(data: pd.DataFrame, players: List[str],
                          metrics: List[str], 
                          player_column: str = 'player_display_name',
                          team_column: str = 'recent_team',
                          backend: str = "matplotlib",
                          plot_type: str = "radar",
                          **kwargs) -> Union[plt.Figure, Any]:
    """Compare players with headshots and team colors.
    
    Args:
        data: DataFrame with player statistics
        players: List of player names to compare
        metrics: List of metric columns to compare
        player_column: Column containing player names
        team_column: Column containing team abbreviations
        backend: Plotting backend ('matplotlib', 'plotly')
        plot_type: Type of comparison plot ('radar', 'bar', 'scatter')
        **kwargs: Additional plotting arguments
        
    Returns:
        matplotlib Figure or plotly Figure
        
    Raises:
        ValueError: If required data is missing
    """
    # Filter data for specified players
    player_data = data[data[player_column].isin(players)].copy()
    
    if player_data.empty:
        raise ValueError(f"No data found for players: {players}")
    
    # Validate columns
    missing_cols = [col for col in metrics if col not in player_data.columns]
    if missing_cols:
        raise ValueError(f"Missing metric columns: {missing_cols}")
    
    if team_column not in player_data.columns:
        warnings.warn(f"Team column '{team_column}' not found. Using generic colors.")
        use_team_colors = False
    else:
        use_team_colors = True
        player_data = clean_team_abbreviations(player_data, team_column)
    
    if backend == "matplotlib":
        return _plot_player_comparison_matplotlib(
            player_data, players, metrics, player_column, 
            team_column, plot_type, use_team_colors, **kwargs
        )
    else:
        raise NotImplementedError("Plotly backend for player comparison not yet implemented")


def _plot_player_comparison_matplotlib(data: pd.DataFrame, players: List[str],
                                     metrics: List[str], player_column: str,
                                     team_column: str, plot_type: str,
                                     use_team_colors: bool, **kwargs) -> plt.Figure:
    """Create matplotlib player comparison plot."""
    
    if plot_type == "radar":
        return _create_radar_chart(
            data, players, metrics, player_column, 
            team_column, use_team_colors, **kwargs
        )
    elif plot_type == "bar":
        return _create_player_bar_chart(
            data, players, metrics, player_column,
            team_column, use_team_colors, **kwargs
        )
    else:
        raise ValueError(f"Unsupported plot_type: {plot_type}")


def _create_radar_chart(data: pd.DataFrame, players: List[str], metrics: List[str],
                       player_column: str, team_column: str, use_team_colors: bool,
                       **kwargs) -> plt.Figure:
    """Create radar chart for player comparison."""
    
    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))
    
    # Number of metrics
    N = len(metrics)
    
    # Compute angle for each metric
    angles = [n / float(N) * 2 * np.pi for n in range(N)]
    angles += angles[:1]  # Complete the circle
    
    # Plot each player
    for i, player in enumerate(players):
        player_row = data[data[player_column] == player].iloc[0]
        
        # Get values for metrics
        values = [player_row[metric] for metric in metrics]
        values += values[:1]  # Complete the circle
        
        # Get color
        if use_team_colors:
            team = player_row[team_column]
            color = get_team_colors(team, "primary")
        else:
            color = f'C{i}'  # Default matplotlib colors
        
        # Plot
        ax.plot(angles, values, 'o-', linewidth=2, label=player, color=color)
        ax.fill(angles, values, alpha=0.25, color=color)
    
    # Add labels
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels([metric.replace('_', ' ').title() for metric in metrics])
    
    # Add legend
    ax.legend(loc='upper right', bbox_to_anchor=(0.1, 0.1))
    
    plt.title("Player Comparison Radar Chart", size=16, fontweight='bold', pad=20)
    plt.tight_layout()
    
    return fig


def _create_player_bar_chart(data: pd.DataFrame, players: List[str], metrics: List[str],
                           player_column: str, team_column: str, use_team_colors: bool,
                           **kwargs) -> plt.Figure:
    """Create bar chart for player comparison."""
    
    n_metrics = len(metrics)
    n_players = len(players)
    
    fig, axes = plt.subplots(1, n_metrics, figsize=(4*n_metrics, 6))
    if n_metrics == 1:
        axes = [axes]
    
    for i, metric in enumerate(metrics):
        ax = axes[i]
        
        # Get data for this metric
        metric_data = []
        colors = []
        
        for player in players:
            player_row = data[data[player_column] == player].iloc[0]
            metric_data.append(player_row[metric])
            
            if use_team_colors:
                team = player_row[team_column]
                colors.append(get_team_colors(team, "primary"))
            else:
                colors.append(f'C{len(metric_data)-1}')
        
        # Create bar chart
        bars = ax.bar(players, metric_data, color=colors, alpha=0.8)
        
        # Styling
        ax.set_title(metric.replace('_', ' ').title(), fontweight='bold')
        ax.tick_params(axis='x', rotation=45)
        
        # Add value labels on bars
        for bar, value in zip(bars, metric_data):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                   f'{value:.2f}', ha='center', va='bottom')
    
    plt.suptitle("Player Comparison", size=16, fontweight='bold')
    plt.tight_layout()
    
    return fig


def plot_game_flow(pbp_data: pd.DataFrame, game_id: str, 
                  backend: str = "matplotlib", **kwargs) -> Union[plt.Figure, Any]:
    """Create game flow visualization with team branding.
    
    Args:
        pbp_data: Play-by-play data from nfl_data_py
        game_id: Game ID to plot
        backend: Plotting backend
        **kwargs: Additional arguments
        
    Returns:
        Game flow visualization
        
    Note:
        This is a placeholder implementation. Full implementation would
        require detailed analysis of nfl_data_py play-by-play structure.
    """
    # Filter for specific game
    game_data = pbp_data[pbp_data['game_id'] == game_id].copy()
    
    if game_data.empty:
        raise ValueError(f"No data found for game_id: {game_id}")
    
    # This is a simplified implementation
    # Real implementation would create win probability or score flow charts
    
    if backend == "matplotlib":
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Placeholder visualization
        ax.plot(range(len(game_data)), game_data.get('score_differential', [0]*len(game_data)))
        ax.set_title(f"Game Flow: {game_id}")
        ax.set_xlabel("Play Number")
        ax.set_ylabel("Score Differential")
        
        return fig
    else:
        raise NotImplementedError("Plotly backend for game flow not yet implemented")


def plot_season_standings(standings_data: pd.DataFrame, season: int,
                         backend: str = "matplotlib", **kwargs) -> Union[plt.Figure, Any]:
    """Standings visualization with team logos.
    
    Args:
        standings_data: Standings data
        season: Season year
        backend: Plotting backend
        **kwargs: Additional arguments
        
    Returns:
        Standings visualization
    """
    # This is a placeholder implementation
    # Real implementation would create standings tables/charts with logos
    
    if backend == "matplotlib":
        fig, ax = plt.subplots(figsize=(12, 10))
        
        # Placeholder bar chart
        teams = standings_data.get('team', [])
        wins = standings_data.get('wins', [])
        
        if teams and wins:
            colors = get_team_colors(teams, "primary")
            bars = ax.barh(teams, wins, color=colors)
            ax.set_title(f"{season} Season Standings")
            ax.set_xlabel("Wins")
        
        return fig
    else:
        raise NotImplementedError("Plotly backend for standings not yet implemented")