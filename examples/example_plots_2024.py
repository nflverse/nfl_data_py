#!/usr/bin/env python3
"""
nflplotpy Example Plots - 2024 NFL Season

This script creates example plots showcasing nflplotpy capabilities:
1. All 32 teams offensive vs defensive EPA per play
2. Division-by-division breakdown (8 subplots)
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.gridspec import GridSpec

# Import nflplotpy and nfl_data_py
import nflplotpy as nflplot

try:
    import nfl_data_py as nfl
    has_nfl_data = True
    print("Using real NFL data from nfl_data_py")
except ImportError:
    has_nfl_data = False
    print("nfl_data_py not available, using realistic sample data")


def create_sample_2024_data():
    """Create realistic sample data for 2024 NFL season."""
    np.random.seed(2024)  # For reproducible results
    
    teams = [
        'ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE',
        'DAL', 'DEN', 'DET', 'GB', 'HOU', 'IND', 'JAC', 'KC',
        'LV', 'LAC', 'LAR', 'MIA', 'MIN', 'NE', 'NO', 'NYG',
        'NYJ', 'PHI', 'PIT', 'SEA', 'SF', 'TB', 'TEN', 'WAS'
    ]
    
    # Create realistic EPA data based on 2024 season performance
    # Good teams have positive offensive EPA, negative defensive EPA (fewer points allowed)
    offensive_epa = np.random.normal(0.02, 0.08, len(teams))  # Mean ~0.02 EPA/play
    defensive_epa = np.random.normal(0.01, 0.06, len(teams))  # Mean ~0.01 EPA/play allowed
    
    # Add some realistic adjustments for known good/bad teams
    team_adjustments = {
        'KC': (0.08, -0.04), 'BUF': (0.06, -0.03), 'SF': (0.05, -0.02),
        'BAL': (0.04, -0.01), 'DET': (0.05, 0.01), 'DAL': (0.03, -0.01),
        'MIA': (0.02, 0.02), 'LAC': (0.01, 0.01), 'PHI': (0.02, 0.00),
        'GB': (0.03, 0.01), 'JAC': (-0.05, 0.04), 'NYG': (-0.04, 0.03),
        'CAR': (-0.06, 0.05), 'NE': (-0.03, 0.02), 'WAS': (-0.02, 0.03),
        'CHI': (-0.04, 0.01), 'TEN': (-0.05, 0.04), 'NYJ': (-0.03, 0.02)
    }
    
    for i, team in enumerate(teams):
        if team in team_adjustments:
            offensive_epa[i] += team_adjustments[team][0]
            defensive_epa[i] += team_adjustments[team][1]
    
    return pd.DataFrame({
        'team': teams,
        'offensive_epa_per_play': offensive_epa,
        'defensive_epa_per_play': defensive_epa
    })


def load_real_2024_data():
    """Load and process real 2024 NFL data."""
    try:
        print("Loading 2024 play-by-play data...")
        pbp = nfl.import_pbp_data([2024])
        
        # Filter for regular season games
        pbp = pbp[pbp['season_type'] == 'REG']
        
        # Calculate offensive EPA per play by team
        offensive_stats = pbp[pbp['posteam'].notna()].groupby('posteam').agg({
            'epa': ['mean', 'count']
        }).round(4)
        offensive_stats.columns = ['offensive_epa_per_play', 'plays']
        offensive_stats = offensive_stats.reset_index()
        offensive_stats.columns = ['team', 'offensive_epa_per_play', 'offensive_plays']
        
        # Calculate defensive EPA per play allowed by team  
        defensive_stats = pbp[pbp['defteam'].notna()].groupby('defteam').agg({
            'epa': ['mean', 'count']
        }).round(4)
        defensive_stats.columns = ['defensive_epa_per_play', 'plays'] 
        defensive_stats = defensive_stats.reset_index()
        defensive_stats.columns = ['team', 'defensive_epa_per_play', 'defensive_plays']
        
        # Merge offensive and defensive stats
        team_stats = pd.merge(offensive_stats, defensive_stats, on='team', how='inner')
        
        # Filter for teams with reasonable play counts (remove weird edge cases)
        team_stats = team_stats[
            (team_stats['offensive_plays'] >= 800) & 
            (team_stats['defensive_plays'] >= 800)
        ]
        
        print(f"Loaded data for {len(team_stats)} teams")
        return team_stats[['team', 'offensive_epa_per_play', 'defensive_epa_per_play']]
        
    except Exception as e:
        print(f"Error loading real data: {e}")
        print("Falling back to sample data...")
        return None


def get_division_teams():
    """Get teams organized by division."""
    return {
        'AFC East': ['BUF', 'MIA', 'NE', 'NYJ'],
        'AFC North': ['BAL', 'CIN', 'CLE', 'PIT'], 
        'AFC South': ['HOU', 'IND', 'JAC', 'TEN'],
        'AFC West': ['DEN', 'KC', 'LV', 'LAC'],
        'NFC East': ['DAL', 'NYG', 'PHI', 'WAS'],
        'NFC North': ['CHI', 'DET', 'GB', 'MIN'],
        'NFC South': ['ATL', 'CAR', 'NO', 'TB'],
        'NFC West': ['ARI', 'LAR', 'SEA', 'SF']
    }


def create_all_teams_plot(data):
    """Create plot with all 32 teams - offensive vs defensive EPA."""
    print("Creating all teams plot...")
    
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Get team colors
    colors = nflplot.get_team_colors(data['team'].tolist(), 'primary')
    
    # Create scatter plot
    scatter = ax.scatter(
        data['offensive_epa_per_play'], 
        data['defensive_epa_per_play'],
        c=colors, 
        s=200, 
        alpha=0.8, 
        edgecolors='white', 
        linewidth=2,
        zorder=3
    )
    
    # Add team labels
    for _, row in data.iterrows():
        ax.annotate(
            row['team'], 
            (row['offensive_epa_per_play'], row['defensive_epa_per_play']),
            xytext=(3, 3), 
            textcoords='offset points', 
            fontsize=9, 
            fontweight='bold',
            color='white',
            bbox=dict(boxstyle='round,pad=0.2', facecolor='black', alpha=0.7)
        )
    
    # Add reference lines at zero
    ax.axhline(y=0, color='gray', linestyle='--', alpha=0.5, zorder=1)
    ax.axvline(x=0, color='gray', linestyle='--', alpha=0.5, zorder=1)
    
    # Add quadrant labels
    ax.text(0.02, 0.98, 'Good Offense\nBad Defense', transform=ax.transAxes, 
            fontsize=10, ha='left', va='top', alpha=0.7,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='lightcoral', alpha=0.3))
    ax.text(0.98, 0.98, 'Good Offense\nGood Defense', transform=ax.transAxes,
            fontsize=10, ha='right', va='top', alpha=0.7,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='lightgreen', alpha=0.3))
    ax.text(0.02, 0.02, 'Bad Offense\nBad Defense', transform=ax.transAxes,
            fontsize=10, ha='left', va='bottom', alpha=0.7,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='lightcoral', alpha=0.3))
    ax.text(0.98, 0.02, 'Bad Offense\nGood Defense', transform=ax.transAxes,
            fontsize=10, ha='right', va='bottom', alpha=0.7,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='lightyellow', alpha=0.3))
    
    # Styling
    ax.set_xlabel('Offensive EPA per Play', fontsize=14, fontweight='bold')
    ax.set_ylabel('Defensive EPA per Play Allowed', fontsize=14, fontweight='bold')
    ax.set_title('2024 NFL Team Performance: Offense vs Defense\nAll 32 Teams', 
                fontsize=16, fontweight='bold', pad=20)
    
    # Apply NFL theme
    nflplot.apply_nfl_theme(ax, style='default')
    
    # Add grid
    ax.grid(True, alpha=0.3, zorder=0)
    
    # Add explanatory text
    fig.text(0.5, 0.02, 
             'Lower defensive EPA is better (fewer points allowed) • Higher offensive EPA is better (more points scored)',
             ha='center', fontsize=10, style='italic', alpha=0.7)
    
    plt.tight_layout()
    plt.savefig('examples/2024_all_teams_epa.png', dpi=300, bbox_inches='tight', facecolor='white')
    print("Saved: examples/2024_all_teams_epa.png")
    
    return fig


def create_division_plots(data):
    """Create 8 subplot figure showing each division separately."""
    print("Creating division plots...")
    
    divisions = get_division_teams()
    
    # Create figure with 2x4 subplot grid
    fig = plt.figure(figsize=(20, 12))
    gs = GridSpec(2, 4, figure=fig, hspace=0.3, wspace=0.25)
    
    for i, (division, teams) in enumerate(divisions.items()):
        # Calculate subplot position
        row = i // 4
        col = i % 4
        ax = fig.add_subplot(gs[row, col])
        
        # Filter data for this division
        div_data = data[data['team'].isin(teams)].copy()
        
        if div_data.empty:
            ax.text(0.5, 0.5, f'No data for\n{division}', 
                   ha='center', va='center', transform=ax.transAxes)
            continue
        
        # Get team colors for this division
        colors = nflplot.get_team_colors(div_data['team'].tolist(), 'primary')
        
        # Create scatter plot
        scatter = ax.scatter(
            div_data['offensive_epa_per_play'],
            div_data['defensive_epa_per_play'],
            c=colors,
            s=300,
            alpha=0.8,
            edgecolors='white',
            linewidth=2
        )
        
        # Add team labels
        for _, row in div_data.iterrows():
            ax.annotate(
                row['team'],
                (row['offensive_epa_per_play'], row['defensive_epa_per_play']),
                xytext=(5, 5),
                textcoords='offset points',
                fontsize=12,
                fontweight='bold',
                color='white',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='black', alpha=0.8)
            )
        
        # Add reference lines
        ax.axhline(y=0, color='gray', linestyle='--', alpha=0.4)
        ax.axvline(x=0, color='gray', linestyle='--', alpha=0.4)
        
        # Styling
        ax.set_title(division, fontsize=14, fontweight='bold', pad=10)
        ax.set_xlabel('Offensive EPA/Play', fontsize=11)
        ax.set_ylabel('Defensive EPA/Play', fontsize=11)
        
        # Apply NFL theme
        nflplot.apply_nfl_theme(ax, style='minimal')
        ax.grid(True, alpha=0.2)
        
        # Set consistent axis limits for comparison across divisions
        ax.set_xlim(data['offensive_epa_per_play'].min() - 0.01, 
                   data['offensive_epa_per_play'].max() + 0.01)
        ax.set_ylim(data['defensive_epa_per_play'].min() - 0.01,
                   data['defensive_epa_per_play'].max() + 0.01)
    
    # Overall title
    fig.suptitle('2024 NFL Team Performance by Division\nOffensive EPA vs Defensive EPA per Play', 
                fontsize=18, fontweight='bold', y=0.95)
    
    # Add explanatory text
    fig.text(0.5, 0.02,
             'Lower defensive EPA is better (fewer points allowed) • Higher offensive EPA is better (more points scored)',
             ha='center', fontsize=12, style='italic', alpha=0.7)
    
    plt.savefig('examples/2024_divisions_epa.png', dpi=300, bbox_inches='tight', facecolor='white')
    print("Saved: examples/2024_divisions_epa.png")
    
    return fig


def create_conference_comparison(data):
    """Bonus plot: AFC vs NFC comparison."""
    print("Creating conference comparison plot...")
    
    # Add conference information
    afc_teams = ['BUF', 'MIA', 'NE', 'NYJ', 'BAL', 'CIN', 'CLE', 'PIT', 
                 'HOU', 'IND', 'JAC', 'TEN', 'DEN', 'KC', 'LV', 'LAC']
    
    data_with_conf = data.copy()
    data_with_conf['conference'] = data_with_conf['team'].apply(
        lambda x: 'AFC' if x in afc_teams else 'NFC'
    )
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    for i, conf in enumerate(['AFC', 'NFC']):
        ax = ax1 if i == 0 else ax2
        conf_data = data_with_conf[data_with_conf['conference'] == conf]
        
        # Get colors
        colors = nflplot.get_team_colors(conf_data['team'].tolist(), 'primary')
        
        # Create scatter plot
        scatter = ax.scatter(
            conf_data['offensive_epa_per_play'],
            conf_data['defensive_epa_per_play'],
            c=colors,
            s=200,
            alpha=0.8,
            edgecolors='white',
            linewidth=2
        )
        
        # Add team labels
        for _, row in conf_data.iterrows():
            ax.annotate(
                row['team'],
                (row['offensive_epa_per_play'], row['defensive_epa_per_play']),
                xytext=(3, 3),
                textcoords='offset points',
                fontsize=9,
                fontweight='bold',
                color='white',
                bbox=dict(boxstyle='round,pad=0.2', facecolor='black', alpha=0.7)
            )
        
        # Add reference lines
        ax.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        ax.axvline(x=0, color='gray', linestyle='--', alpha=0.5)
        
        # Styling
        ax.set_title(f'{conf} Conference', fontsize=14, fontweight='bold')
        ax.set_xlabel('Offensive EPA per Play', fontsize=12)
        ax.set_ylabel('Defensive EPA per Play', fontsize=12)
        
        # Apply NFL theme
        conf_color = '#FF0000' if conf == 'AFC' else '#0000FF'
        nflplot.apply_nfl_theme(ax, style='default')
        ax.grid(True, alpha=0.3)
        
        # Set consistent limits
        ax.set_xlim(data['offensive_epa_per_play'].min() - 0.01, 
                   data['offensive_epa_per_play'].max() + 0.01)
        ax.set_ylim(data['defensive_epa_per_play'].min() - 0.01,
                   data['defensive_epa_per_play'].max() + 0.01)
    
    plt.suptitle('2024 NFL Performance: AFC vs NFC', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('examples/2024_conferences_epa.png', dpi=300, bbox_inches='tight', facecolor='white')
    print("Saved: examples/2024_conferences_epa.png")
    
    return fig


def main():
    """Create all example plots."""
    print("nflplotpy 2024 NFL Example Plots")
    print("=" * 40)
    
    # Create examples directory
    import os
    os.makedirs('examples', exist_ok=True)
    
    # Load data
    if has_nfl_data:
        data = load_real_2024_data()
        if data is None:
            data = create_sample_2024_data()
    else:
        data = create_sample_2024_data()
    
    print(f"\nData summary:")
    print(f"Teams: {len(data)}")
    print(f"Offensive EPA range: {data['offensive_epa_per_play'].min():.3f} to {data['offensive_epa_per_play'].max():.3f}")
    print(f"Defensive EPA range: {data['defensive_epa_per_play'].min():.3f} to {data['defensive_epa_per_play'].max():.3f}")
    print()
    
    # Create plots
    fig1 = create_all_teams_plot(data)
    plt.close(fig1)
    
    fig2 = create_division_plots(data)
    plt.close(fig2)
    
    fig3 = create_conference_comparison(data)
    plt.close(fig3)
    
    print("\n" + "=" * 40)
    print("All plots created successfully!")
    print("\nGenerated files:")
    print("- examples/2024_all_teams_epa.png")
    print("- examples/2024_divisions_epa.png") 
    print("- examples/2024_conferences_epa.png")
    print("\nThese plots demonstrate:")
    print("✓ Team colors and branding")
    print("✓ High-quality NFL visualizations")
    print("✓ Multiple subplot layouts")
    print("✓ Professional styling and themes")
    print("✓ Integration with NFL data")


if __name__ == "__main__":
    main()