#!/usr/bin/env python3
"""
nflplotpy Examples - 2024 NFL Season

This script creates example plots using NFL data from nfl_data_py:
1. All 32 teams offensive vs defensive EPA per play (2024 season)
2. Division-by-division breakdown (8 subplots)
3. Conference comparison

Uses actual play-by-play data aggregated over the 2024 regular season.
Automatically caches processed data for faster subsequent runs.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.gridspec import GridSpec
import os
import sys

# Import nflplotpy and nfl_data_py
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
import nflplotpy as nflplot
import nfl_data_py as nfl


def load_and_process_2024_data():
    """Load 2024 NFL team EPA data - uses cached data if available, otherwise downloads fresh data."""
    cache_file = os.path.join(os.path.dirname(__file__), '2024_nfl_team_epa_data.csv')
    
    if os.path.exists(cache_file):
        print(f"✅ Loading cached 2024 data from: {cache_file}")
        team_stats = pd.read_csv(cache_file)
        print(f"Loaded cached data for {len(team_stats)} teams")
    else:
        print("📥 Cached data not found. Downloading fresh 2024 NFL play-by-play data...")
        
        # Load 2024 regular season data
        pbp = nfl.import_pbp_data([2024])
        print(f"Loaded {len(pbp):,} plays from 2024 season")
        
        # Filter for regular season only
        pbp_reg = pbp[pbp['season_type'] == 'REG'].copy()
        print(f"Regular season plays: {len(pbp_reg):,}")
        
        # Remove plays with missing EPA or team data
        pbp_clean = pbp_reg[
            (pbp_reg['epa'].notna()) & 
            (pbp_reg['posteam'].notna()) & 
            (pbp_reg['defteam'].notna())
        ].copy()
        print(f"Clean plays with EPA data: {len(pbp_clean):,}")
        
        # Calculate offensive EPA per play by team
        print("Calculating offensive EPA per play...")
        offensive_stats = pbp_clean.groupby('posteam').agg({
            'epa': ['mean', 'count', 'sum'],
            'play_id': 'count'
        }).round(4)
        
        offensive_stats.columns = ['off_epa_per_play', 'off_epa_count', 'off_total_epa', 'off_total_plays']
        offensive_stats = offensive_stats.reset_index()
        offensive_stats.columns = ['team', 'off_epa_per_play', 'off_epa_count', 'off_total_epa', 'off_total_plays']
        
        # Calculate defensive EPA per play allowed by team  
        print("Calculating defensive EPA per play allowed...")
        defensive_stats = pbp_clean.groupby('defteam').agg({
            'epa': ['mean', 'count', 'sum'],
            'play_id': 'count'
        }).round(4)
        
        defensive_stats.columns = ['def_epa_per_play', 'def_epa_count', 'def_total_epa', 'def_total_plays']
        defensive_stats = defensive_stats.reset_index()
        defensive_stats.columns = ['team', 'def_epa_per_play', 'def_epa_count', 'def_total_epa', 'def_total_plays']
        
        # Merge offensive and defensive stats
        team_stats = pd.merge(offensive_stats, defensive_stats, on='team', how='inner')
        
        # Filter for teams with reasonable play counts (removes weird edge cases)
        min_plays = 800  # Reasonable threshold for a full season
        team_stats = team_stats[
            (team_stats['off_total_plays'] >= min_plays) & 
            (team_stats['def_total_plays'] >= min_plays)
        ]
        
        # Save the processed data for next time
        team_stats.to_csv(cache_file, index=False)
        print(f"💾 Cached processed data to: {cache_file}")
    
    # Display summary info
    print(f"Final dataset: {len(team_stats)} teams")
    print(f"Offensive EPA range: {team_stats['off_epa_per_play'].min():.3f} to {team_stats['off_epa_per_play'].max():.3f}")
    print(f"Defensive EPA range: {team_stats['def_epa_per_play'].min():.3f} to {team_stats['def_epa_per_play'].max():.3f}")
    
    # Display top/bottom teams
    print("\nTop 5 Offensive Teams (EPA/play):")
    top_off = team_stats.nlargest(5, 'off_epa_per_play')[['team', 'off_epa_per_play']]
    print(top_off.to_string(index=False))
    
    print("\nTop 5 Defensive Teams (lowest EPA/play allowed):")
    top_def = team_stats.nsmallest(5, 'def_epa_per_play')[['team', 'def_epa_per_play']]
    print(top_def.to_string(index=False))
    
    return team_stats[['team', 'off_epa_per_play', 'def_epa_per_play']]


def get_division_teams():
    """Get teams organized by division.
    
    Uses the team abbreviations that match nfl_data_py output:
    - JAX (not JAC) for Jacksonville Jaguars
    - LA (not LAR) for Los Angeles Rams
    """
    return {
        'AFC East': ['BUF', 'MIA', 'NE', 'NYJ'],
        'AFC North': ['BAL', 'CIN', 'CLE', 'PIT'], 
        'AFC South': ['HOU', 'IND', 'JAX', 'TEN'],  # JAX not JAC
        'AFC West': ['DEN', 'KC', 'LV', 'LAC'],
        'NFC East': ['DAL', 'NYG', 'PHI', 'WAS'],
        'NFC North': ['CHI', 'DET', 'GB', 'MIN'],
        'NFC South': ['ATL', 'CAR', 'NO', 'TB'],
        'NFC West': ['ARI', 'LA', 'SEA', 'SF']  # LA not LAR
    }


def create_all_teams_plot(data, show_logos=True):
    """Create plot with all 32 teams - offensive vs defensive EPA."""
    print("Creating all teams plot...")
    
    fig, ax = plt.subplots(figsize=(18, 14))
    
    # Get team colors - validate teams first
    valid_teams = nflplot.validate_teams(data['team'].tolist(), allow_conferences=False)
    colors = nflplot.get_team_colors(valid_teams, 'primary')
    
    if show_logos:
        # Create invisible scatter plot for positioning
        scatter = ax.scatter(
            data['off_epa_per_play'], 
            data['def_epa_per_play'],
            c='white', s=1, alpha=0.01,  # Nearly invisible
            zorder=1
        )
        
        # Add NFL team logos
        from nflplotpy.matplotlib.artists import add_nfl_logos
        try:
            logos = add_nfl_logos(
                ax, 
                data['team'].tolist(), 
                data['off_epa_per_play'].values, 
                data['def_epa_per_play'].values, 
                target_width_pixels=60,
                alpha=0.9
            )
            successful_logos = len([l for l in logos if l is not None])
            print(f"✅ Successfully added {successful_logos} team logos")
        except Exception as e:
            print(f"⚠️ Logo rendering had issues: {e}")
            # Fall back to colored dots
            show_logos = False
    
    if not show_logos:
        # Traditional scatter plot with team colors
        scatter = ax.scatter(
            data['off_epa_per_play'], 
            data['def_epa_per_play'],
            c=colors, 
            s=200, 
            alpha=0.8, 
            edgecolors='white', 
            linewidth=2,
            zorder=3
        )
        
        # Add team labels for dots
        for _, row in data.iterrows():
            ax.annotate(
                row['team'], 
                (row['off_epa_per_play'], row['def_epa_per_play']),
                xytext=(3, 3), 
                textcoords='offset points', 
                fontsize=9, 
                fontweight='bold',
                color='white',
                bbox=dict(boxstyle='round,pad=0.2', facecolor='black', alpha=0.7)
            )
    
    # Add reference lines at league averages
    league_avg_off = data['off_epa_per_play'].mean()
    league_avg_def = data['def_epa_per_play'].mean()
    ax.axhline(y=league_avg_def, color='gray', linestyle='--', alpha=0.6, zorder=1, label=f'League Avg Defense ({league_avg_def:.3f})')
    ax.axvline(x=league_avg_off, color='gray', linestyle='--', alpha=0.6, zorder=1, label=f'League Avg Offense ({league_avg_off:.3f})')
    
    # Add quadrant background colors using rectangles
    from matplotlib.patches import Rectangle
    xlims = ax.get_xlim()
    ylims = ax.get_ylim()
    
    # Q1 (upper right): Good Offense, Poor Defense - Orange (barnburner games)
    rect1 = Rectangle((league_avg_off, league_avg_def), xlims[1] - league_avg_off, ylims[1] - league_avg_def, 
                      alpha=0.18, color='darkorange', zorder=0)
    ax.add_patch(rect1)
    
    # Q2 (lower right): Good Offense, Good Defense - Blue (blue chip teams)  
    rect2 = Rectangle((league_avg_off, ylims[0]), xlims[1] - league_avg_off, league_avg_def - ylims[0],
                      alpha=0.18, color='royalblue', zorder=0)
    ax.add_patch(rect2)
    
    # Q3 (lower left): Poor Offense, Good Defense - Yellow (grind 'em out games)
    rect3 = Rectangle((xlims[0], ylims[0]), league_avg_off - xlims[0], league_avg_def - ylims[0],
                      alpha=0.18, color='goldenrod', zorder=0)
    ax.add_patch(rect3)
    
    # Q4 (upper left): Poor Offense, Poor Defense - Red (bad teams)
    rect4 = Rectangle((xlims[0], league_avg_def), league_avg_off - xlims[0], ylims[1] - league_avg_def,
                      alpha=0.18, color='crimson', zorder=0)
    ax.add_patch(rect4)
    
    # Add quadrant labels with better contrast
    ax.text(0.02, 0.98, 'Poor Offense\nPoor Defense\n"Scouting the SEC"', transform=ax.transAxes, 
            fontsize=12, ha='left', va='top', alpha=1.0, fontweight='bold', color='white',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='darkred', alpha=0.9, edgecolor='white', linewidth=2))
    ax.text(0.98, 0.98, 'Good Offense\nPoor Defense\n"Barn burners"', transform=ax.transAxes,
            fontsize=12, ha='right', va='top', alpha=1.0, fontweight='bold', color='white',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='darkorange', alpha=0.9, edgecolor='white', linewidth=2))
    ax.text(0.02, 0.02, 'Poor Offense\nGood Defense\n"Grind It Out"', transform=ax.transAxes,
            fontsize=12, ha='left', va='bottom', alpha=1.0, fontweight='bold', color='black',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='gold', alpha=0.9, edgecolor='black', linewidth=2))
    ax.text(0.98, 0.02, 'Good Offense\nGood Defense\n"Blue Chip"', transform=ax.transAxes,
            fontsize=12, ha='right', va='bottom', alpha=1.0, fontweight='bold', color='white',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='navy', alpha=0.9, edgecolor='white', linewidth=2))
    
    # Styling
    ax.set_xlabel('Offensive EPA per Play', fontsize=14, fontweight='bold')
    ax.set_ylabel('Defensive EPA per Play Allowed', fontsize=14, fontweight='bold')
    ax.set_title('2024 NFL Team EPA Analysis\nOffensive vs Defensive (allowed) EPA/Play (Regular Season)', 
                fontsize=16, fontweight='bold', pad=20)
    
    # Apply NFL theme
    nflplot.apply_nfl_theme(ax, style='default')
    
    # Add grid
    ax.grid(True, alpha=0.3, zorder=0)
    
    plt.tight_layout(rect=[0, 0.05, 1, 1])  # Leave more space at bottom (1/4 height)
    
    # Add explanatory text with more space
    fig.text(0.5, 0.04, 
             'Data: 2024 NFL Regular Season Play-by-Play • Lower defensive EPA is better • Higher offensive EPA is better',
             ha='center', fontsize=11, style='italic', alpha=0.7)
    
    # Save in nflplotpy examples directory
    output_dir = os.path.dirname(__file__)
    output_path = os.path.join(output_dir, '2024_all_teams_epa.png')
    plt.savefig(output_path, dpi=600, bbox_inches='tight', facecolor='white')
    print(f"Saved: {output_path}")
    
    return fig


def create_division_plots(data, show_logos=True):
    """Create 8 subplot figure showing each division separately."""
    print("Creating division plots with 2024 pbp data...")
    if show_logos:
        print("🏈 Using team logos in division breakdown!")
    
    divisions = get_division_teams()
    
    # Create figure with 2x4 subplot grid
    fig = plt.figure(figsize=(28, 16))
    gs = GridSpec(2, 4, figure=fig, hspace=0.4, wspace=0.35)
    
    for i, (division, teams) in enumerate(divisions.items()):
        # Calculate subplot position
        row = i // 4
        col = i % 4
        ax = fig.add_subplot(gs[row, col])
        
        # Filter data for this division - only include teams present in our data
        available_teams = [t for t in teams if t in data['team'].values]
        div_data = data[data['team'].isin(available_teams)].copy()
        
        if div_data.empty:
            ax.text(0.5, 0.5, f'No data for\n{division}', 
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_title(division, fontsize=14, fontweight='bold', pad=10)
            continue
        
        # Get team colors for this division
        colors = nflplot.get_team_colors(div_data['team'].tolist(), 'primary')
        
        if show_logos:
            # Create invisible scatter plot for positioning
            ax.scatter(
                div_data['off_epa_per_play'],
                div_data['def_epa_per_play'],
                c='white', s=1, alpha=0.01
            )
            
            # Add team logos
            from nflplotpy.matplotlib.artists import add_nfl_logos
            try:
                logos = add_nfl_logos(
                    ax, 
                    div_data['team'].tolist(), 
                    div_data['off_epa_per_play'].values, 
                    div_data['def_epa_per_play'].values, 
                    target_width_pixels=65  # Higher resolution for division plots
                )
            except Exception as e:
                print(f"⚠️ Division {division} logo issues: {e}")
                show_logos = False  # Fall back for this division
        
        if not show_logos:
            # Traditional scatter plot
            scatter = ax.scatter(
                div_data['off_epa_per_play'],
                div_data['def_epa_per_play'],
                c=colors,
                s=300,
                alpha=0.8,
                edgecolors='white',
                linewidth=2
            )
            
            # Add team labels for dots
            for _, row in div_data.iterrows():
                ax.annotate(
                    row['team'],
                    (row['off_epa_per_play'], row['def_epa_per_play']),
                    xytext=(5, 5),
                    textcoords='offset points',
                    fontsize=12,
                    fontweight='bold',
                    color='white',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='black', alpha=0.8)
                )
        
        # Add reference lines - league and division averages
        league_avg_off = data['off_epa_per_play'].mean()
        league_avg_def = data['def_epa_per_play'].mean()
        div_avg_off = div_data['off_epa_per_play'].mean()
        div_avg_def = div_data['def_epa_per_play'].mean()
        
        # League averages (gray dashed)
        ax.axhline(y=league_avg_def, color='gray', linestyle='--', alpha=0.5, label='League Avg')
        ax.axvline(x=league_avg_off, color='gray', linestyle='--', alpha=0.5)
        
        # Division averages with conference colors
        div_color = 'red' if 'AFC' in division else 'blue'
        ax.axhline(y=div_avg_def, color=div_color, linestyle=':', alpha=0.8, linewidth=2, label=f'{division} Avg')
        ax.axvline(x=div_avg_off, color=div_color, linestyle=':', alpha=0.8, linewidth=2)
        
        # Styling
        ax.set_title(f'{division}', fontsize=12, fontweight='bold', pad=10)
        ax.set_xlabel('Offensive EPA/Play', fontsize=10)
        ax.set_ylabel('Defensive EPA/Play', fontsize=10)
        
        # Apply NFL theme
        nflplot.apply_nfl_theme(ax, style='minimal')
        ax.grid(True, alpha=0.2)
        
        # Add legend for reference lines
        ax.legend(loc='upper right', fontsize=8, framealpha=0.9)
        
        # Set consistent axis limits with extra padding for logos
        x_range = data['off_epa_per_play'].max() - data['off_epa_per_play'].min()
        y_range = data['def_epa_per_play'].max() - data['def_epa_per_play'].min()
        ax.set_xlim(data['off_epa_per_play'].min() - x_range * 0.05, 
                   data['off_epa_per_play'].max() + x_range * 0.05)
        ax.set_ylim(data['def_epa_per_play'].min() - y_range * 0.05,
                   data['def_epa_per_play'].max() + y_range * 0.05)
    
    # Overall title
    fig.suptitle('2024 NFL Team Performance by Division\nOffensive vs Defensive EPA per Play (Regular Season)', 
                fontsize=18, fontweight='bold', y=0.98)
    
    # Add explanatory text
    fig.text(0.5, 0.01,
             'Data: 2024 NFL Regular Season Play-by-Play • Lower defensive EPA = better defense • Higher offensive EPA = better offense',
             ha='center', fontsize=12, style='italic', alpha=0.7)
    
    # Save in nflplotpy examples directory
    output_dir = os.path.dirname(__file__)
    output_path = os.path.join(output_dir, '2024_divisions_epa.png')
    plt.savefig(output_path, dpi=600, bbox_inches='tight', facecolor='white')
    print(f"Saved: {output_path}")
    
    return fig


def create_conference_comparison(data, show_logos=True):
    """Create AFC vs NFC comparison."""
    print("Creating conference comparison with 2024 pbp data...")
    if show_logos:
        print("🏈 Using team logos in conference comparison!")
    
    # Define AFC teams (using correct abbreviations from data)
    afc_teams = ['BUF', 'MIA', 'NE', 'NYJ', 'BAL', 'CIN', 'CLE', 'PIT', 
                 'HOU', 'IND', 'JAX', 'TEN', 'DEN', 'KC', 'LV', 'LAC']  # JAX not JAC
    
    # Add conference column
    data_with_conf = data.copy()
    data_with_conf['conference'] = data_with_conf['team'].apply(
        lambda x: 'AFC' if x in afc_teams else 'NFC'
    )
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 12))
    
    for i, conf in enumerate(['AFC', 'NFC']):
        ax = ax1 if i == 0 else ax2
        conf_data = data_with_conf[data_with_conf['conference'] == conf]
        
        if conf_data.empty:
            ax.text(0.5, 0.5, f'No data for {conf}', ha='center', va='center', transform=ax.transAxes)
            continue
        
        # Get colors
        colors = nflplot.get_team_colors(conf_data['team'].tolist(), 'primary')
        
        if show_logos:
            # Create invisible scatter plot for positioning
            ax.scatter(
                conf_data['off_epa_per_play'],
                conf_data['def_epa_per_play'],
                c='white', s=1, alpha=0.01
            )
            
            # Add team logos
            from nflplotpy.matplotlib.artists import add_nfl_logos
            try:
                logos = add_nfl_logos(
                    ax, 
                    conf_data['team'].tolist(), 
                    conf_data['off_epa_per_play'].values, 
                    conf_data['def_epa_per_play'].values, 
                    target_width_pixels=62
                )
            except Exception as e:
                print(f"⚠️ Conference {conf} logo issues: {e}")
                show_logos = False  # Fall back for this conference
        
        if not show_logos:
            # Traditional scatter plot
            scatter = ax.scatter(
                conf_data['off_epa_per_play'],
                conf_data['def_epa_per_play'],
                c=colors,
                s=200,
                alpha=0.8,
                edgecolors='white',
                linewidth=2
            )
            
            # Add team labels for dots
            for _, row in conf_data.iterrows():
                ax.annotate(
                    row['team'],
                    (row['off_epa_per_play'], row['def_epa_per_play']),
                    xytext=(3, 3),
                    textcoords='offset points',
                    fontsize=9,
                    fontweight='bold',
                    color='white',
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='black', alpha=0.7)
                )
        
        # Calculate conference averages
        avg_off = conf_data['off_epa_per_play'].mean()
        avg_def = conf_data['def_epa_per_play'].mean()
        
        # Add reference lines - both league and conference averages
        league_avg_off = data['off_epa_per_play'].mean()
        league_avg_def = data['def_epa_per_play'].mean()
        ax.axhline(y=league_avg_def, color='gray', linestyle='--', alpha=0.4, label='League Avg')
        ax.axvline(x=league_avg_off, color='gray', linestyle='--', alpha=0.4)
        
        # Add conference-specific averages with proper colors
        conf_color = 'red' if conf == 'AFC' else 'blue'
        ax.axhline(y=avg_def, color=conf_color, linestyle=':', alpha=0.8, linewidth=2, label=f'{conf} Avg')
        ax.axvline(x=avg_off, color=conf_color, linestyle=':', alpha=0.8, linewidth=2)
        
        # Styling
        ax.set_title(f'{conf} Conference', fontsize=14, fontweight='bold')
        ax.set_xlabel('Offensive EPA per Play', fontsize=12)
        ax.set_ylabel('Defensive EPA per Play', fontsize=12)
        
        # Add conference averages as text
        ax.text(0.02, 0.02, f'Avg Offense: {avg_off:.3f}\nAvg Defense: {avg_def:.3f}', 
                transform=ax.transAxes, fontsize=10, 
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
        
        # Apply NFL theme
        nflplot.apply_nfl_theme(ax, style='default')
        ax.grid(True, alpha=0.3)
        
        # Add legend for reference lines
        ax.legend(loc='upper right', fontsize=9, framealpha=0.9)
        
        # Set consistent limits with extra padding for logos
        x_range = data['off_epa_per_play'].max() - data['off_epa_per_play'].min()
        y_range = data['def_epa_per_play'].max() - data['def_epa_per_play'].min()
        ax.set_xlim(data['off_epa_per_play'].min() - x_range * 0.05, 
                   data['off_epa_per_play'].max() + x_range * 0.05)
        ax.set_ylim(data['def_epa_per_play'].min() - y_range * 0.05,
                   data['def_epa_per_play'].max() + y_range * 0.05)
    
    plt.suptitle('2024 NFL AFC vs NFC Performance Comparison', fontsize=16, fontweight='bold')
    plt.tight_layout()
    
    # Save in nflplotpy examples directory
    output_dir = os.path.dirname(__file__)
    output_path = os.path.join(output_dir, '2024_conferences_epa.png')
    plt.savefig(output_path, dpi=600, bbox_inches='tight', facecolor='white')
    print(f"Saved: {output_path}")
    
    return fig


def main():
    """Create all example plots using 2024 NFL data."""
    print("nflplotpy Examples - 2024 NFL Season")
    print("=" * 50)
    try:
        # Load and process data (cached if available)
        data = load_and_process_2024_data()
        print(f"\nSuccessfully processed data for {len(data)} teams")
        
        # Create plots with team logos enabled!
        print("\n" + "=" * 50)
        print("🏈 Creating all plots with team logos enabled!")
        show_logos = True  # The key setting - enables logos instead of dots
        
        fig1 = create_all_teams_plot(data, show_logos=show_logos)
        plt.close(fig1)
        
        fig2 = create_division_plots(data, show_logos=show_logos)
        plt.close(fig2)
        
        fig3 = create_conference_comparison(data, show_logos=show_logos)
        plt.close(fig3)
        
        print("\n" + "=" * 50)
        print("✅ All plots created successfully using 2024 NFL data.")
        print("\nGenerated files:")
        print("- nflplotpy/examples/2024_all_teams_epa.png")
        print("- nflplotpy/examples/2024_divisions_epa.png") 
        print("- nflplotpy/examples/2024_conferences_epa.png")
        print("\n📊 These plots showcase:")
        print("✓ 2024 NFL play-by-play data aggregated by team")
        print("✓ Authentic team performance metrics (EPA per play)")
        print("✓ Professional NFL visualization styling")
        print("✓ Publication-quality output suitable for analysis")
        print("\n🏈 Data source: nfl_data_py 2024 regular season play-by-play")
        
    except Exception as e:
        print(f"\n❌ Error creating plots: {e}")
        print("Make sure you have nfl_data_py installed and working internet connection")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()