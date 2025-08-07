#!/usr/bin/env python3
"""
nflplotpy Demo Script - Complete Feature Showcase
==================================================

This comprehensive demo script showcases all major features of nflplotpy,
the Python equivalent of R's nflplotR package for NFL data visualization.

Features Demonstrated:
- Team logo integration with matplotlib plots
- NFL color palettes and team branding
- High-level plotting functions for quick visualizations
- Integration with real NFL data from nfl_data_py
- Asset management and caching system
- Multiple visualization backends (matplotlib, plotly)

Requirements:
- nflplotpy (included in this repository)
- nfl_data_py for real NFL data
- matplotlib for plotting
- pandas for data manipulation
- numpy for sample data generation
- plotly (optional) for interactive plots

Usage:
    python nflplotpy_demo.py

This will generate several example plots demonstrating different features.
All plots are saved to the examples/ directory for easy viewing.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

# Import nflplotpy and nfl_data_py
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
import nflplotpy as nflplot
import nfl_data_py as nfl

# Set up matplotlib for better plots
plt.style.use('default')
plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.size'] = 10


def demo_basic_functionality():
    """
    Demonstrate basic nflplotpy functionality.
    
    This section covers:
    - Getting team colors (single and multiple teams)
    - Team utility functions
    - Asset management and caching
    """
    print("🏈 BASIC FUNCTIONALITY DEMO")
    print("=" * 50)
    
    # 1. Team Colors - The foundation of NFL visualizations
    print("\n1. 🎨 Team Colors:")
    print("   Getting official NFL team colors for data visualization")
    
    # Single team color
    ari_color = nflplot.get_team_colors('ARI', 'primary')
    print(f"   Arizona Cardinals primary: {ari_color}")
    
    # Multiple teams at once (efficient for plotting)
    teams_sample = ['KC', 'GB', 'DAL', 'NE']
    multiple_colors = nflplot.get_team_colors(teams_sample, 'primary')
    print(f"   Sample team colors: {dict(zip(teams_sample, multiple_colors))}")
    
    # Secondary colors are also available
    sf_secondary = nflplot.get_team_colors('SF', 'secondary')
    print(f"   San Francisco 49ers secondary: {sf_secondary}")
    
    # 2. Team Utilities - Helpful functions for organizing teams
    print("\n2. 🏟️  Team Organization:")
    print("   Functions to group and organize NFL teams")
    
    # Team factor for categorical data
    teams = ['KC', 'BUF', 'GB', 'DAL']
    factor = nflplot.team_factor(teams)
    print(f"   Team factor for {teams}: {factor}")
    
    # Conference and division groupings
    conf_tiers = nflplot.team_tiers('conference')
    print(f"   Available conferences: {list(conf_tiers.keys())}")
    
    div_tiers = nflplot.team_tiers('division') 
    print(f"   Number of divisions: {len(div_tiers)}")
    print(f"   Sample division (AFC West): {div_tiers.get('AFC West', 'Not found')}")
    
    # 3. Asset Management - Logo and image caching system
    print("\n3. 💾 Asset Management:")
    print("   Caching system for team logos and graphics")
    
    manager = nflplot.NFLAssetManager()
    cache_info = manager.get_cache_info()
    print(f"   Cache location: {cache_info['cache_dir']}")
    print(f"   Cached logos: {cache_info['logos_count']}")
    print(f"   Cache size: {cache_info['total_size_bytes']} bytes")
    
    if cache_info['logos_count'] == 0:
        print("   💡 Logos will be downloaded and cached on first use")
    else:
        print("   ✅ Logos cached and ready for fast plotting")
    
    print("\n   📝 All assets are cached locally for fast subsequent use")


def demo_matplotlib_integration():
    """
    Demonstrate matplotlib integration with NFL-specific features.
    
    This section covers:
    - Creating scatter plots with team colors
    - Adding reference lines (median/mean)  
    - Applying NFL themes and styling
    - Comparing dots vs logos visualization
    """
    print("\n📊 MATPLOTLIB INTEGRATION DEMO")
    print("=" * 50)
    
    # Create realistic sample team data
    print("\n1. 📋 Generating sample team performance data...")
    teams = ['KC', 'BUF', 'GB', 'DAL', 'SF', 'NE', 'BAL', 'PIT']
    np.random.seed(42)  # For reproducible demo
    
    sample_data = pd.DataFrame({
        'team': teams,
        'epa_per_play': np.random.normal(0, 0.08, len(teams)),
        'success_rate': np.random.normal(0.45, 0.04, len(teams)),
        'points_per_game': np.random.normal(24, 4, len(teams)),
        'turnover_margin': np.random.normal(0, 1.5, len(teams))
    })
    
    print(f"   Generated data for {len(teams)} teams")
    print("   Sample data preview:")
    print(sample_data.head(3).to_string(index=False))
    
    # 2. Traditional scatter plot with team colors (dots)
    print("\n2. 🔴 Creating scatter plot with team-colored dots...")
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
    
    # LEFT PLOT: Team colored dots (traditional approach)
    colors = nflplot.get_team_colors(sample_data['team'].tolist(), 'primary')
    
    scatter1 = ax1.scatter(
        sample_data['epa_per_play'], 
        sample_data['success_rate'],
        c=colors, 
        s=180, 
        alpha=0.8, 
        edgecolors='white', 
        linewidth=2
    )
    
    # Add reference lines (median lines are common in NFL analytics)
    nflplot.add_median_lines(ax1, sample_data['epa_per_play'].values, axis='x', alpha=0.6, color='gray')
    nflplot.add_median_lines(ax1, sample_data['success_rate'].values, axis='y', alpha=0.6, color='gray')
    
    # Apply NFL theme for professional appearance
    nflplot.apply_nfl_theme(ax1, style='default')
    
    # Add team labels
    for _, row in sample_data.iterrows():
        ax1.annotate(row['team'], 
                    (row['epa_per_play'], row['success_rate']),
                    xytext=(5, 5), textcoords='offset points', 
                    fontsize=9, fontweight='bold',
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.8))
    
    ax1.set_xlabel('EPA per Play', fontsize=11)
    ax1.set_ylabel('Success Rate', fontsize=11)
    ax1.set_title('Traditional: Team-Colored Dots', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    
    # RIGHT PLOT: Team logos (modern approach)
    print("   🏈 Adding team logos to comparison plot...")
    
    # Create invisible scatter for positioning
    ax2.scatter(
        sample_data['epa_per_play'], 
        sample_data['success_rate'],
        c='white', s=1, alpha=0.01
    )
    
    # Add logos (this is the key feature!)
    from nflplotpy.matplotlib.artists import add_nfl_logos
    try:
        logos = add_nfl_logos(
            ax2, 
            sample_data['team'].tolist(), 
            sample_data['epa_per_play'].values, 
            sample_data['success_rate'].values, 
            width=0.12
        )
        successful_logos = len([l for l in logos if l is not None])
        print(f"   ✅ Successfully added {successful_logos} team logos")
    except Exception as e:
        print(f"   ⚠️  Logo rendering had issues: {e}")
    
    # Same styling as left plot for comparison
    nflplot.add_median_lines(ax2, sample_data['epa_per_play'].values, axis='x', alpha=0.6, color='gray')
    nflplot.add_median_lines(ax2, sample_data['success_rate'].values, axis='y', alpha=0.6, color='gray')
    nflplot.apply_nfl_theme(ax2, style='default')
    
    ax2.set_xlabel('EPA per Play', fontsize=11)
    ax2.set_ylabel('Success Rate', fontsize=11) 
    ax2.set_title('Modern: Team Logos', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    # Overall plot styling
    plt.suptitle('nflplotpy Matplotlib Integration: Dots vs Logos', fontsize=14, fontweight='bold')
    plt.tight_layout()
    
    # Save the demonstration plot
    output_path = 'examples/matplotlib_integration_demo.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"   💾 Saved plot: {output_path}")
    plt.close()
    
    print("\n   📝 Key Features Demonstrated:")
    print("   • Team-specific colors using official NFL palettes")
    print("   • Reference lines for statistical context")
    print("   • Professional NFL theme styling")
    print("   • Modern logo integration vs traditional dots")
    print("   • High-DPI output suitable for presentations")


def demo_high_level_plotting():
    """Demonstrate high-level plotting functions."""
    print("\n=== High-Level Plotting Demo ===")
    
    # Create sample data
    teams = ['KC', 'BUF', 'GB', 'TB', 'BAL', 'SEA', 'TEN', 'IND']
    np.random.seed(123)
    
    team_stats = pd.DataFrame({
        'team': teams,
        'offensive_epa': np.random.normal(0.1, 0.08, len(teams)),
        'defensive_epa': np.random.normal(-0.05, 0.06, len(teams)),
        'win_rate': np.random.uniform(0.3, 0.9, len(teams))
    })
    
    # Use high-level plotting function with LOGOS ENABLED
    print("   🚀 Creating plot with high-level function...")
    fig = nflplot.plot_team_stats(
        team_stats,
        x='offensive_epa',
        y='defensive_epa', 
        backend='matplotlib',
        show_logos=True,  # 🔑 KEY FEATURE: Enable team logos!
        logo_size=0.15,   # Adjust logo size as needed
        add_reference_lines=True,
        reference_type='both',  # Shows both median and mean lines
        title='High-Level Function: Team Logos Enabled\nOffensive vs Defensive EPA',
        figsize=(12, 8)
    )
    
    fig.savefig('examples/high_level_team_plot.png', dpi=300, bbox_inches='tight')
    print("Saved high-level plotting demo to 'examples/high_level_team_plot.png'")
    plt.close(fig)


def demo_color_palettes():
    """Demonstrate color palette functionality."""
    print("\n=== Color Palette Demo ===")
    
    # Create palette manager
    palette = nflplot.NFLColorPalette()
    
    # 1. Conference colors
    afc_colors = palette.create_conference_palette('AFC')
    nfc_colors = palette.create_conference_palette('NFC')
    
    print(f"AFC teams color count: {len(afc_colors)}")
    print(f"NFC teams color count: {len(nfc_colors)}")
    
    # 2. Division colors
    nfc_west_colors = palette.create_division_palette('NFC West')
    print(f"NFC West colors: {nfc_west_colors}")
    
    # 3. Create gradient
    gradient = palette.create_gradient('KC', 'SF', n_colors=10)
    print(f"KC to SF gradient: {gradient[:3]}...{gradient[-3:]}")
    
    # 4. Matplotlib colormap
    teams = ['KC', 'BUF', 'GB', 'TB']
    cmap = nflplot.create_nfl_colormap(teams)
    print(f"Created colormap with {len(cmap.colors)} colors")


def demo_with_real_nfl_data():
    """Demonstrate with real NFL data."""
    print("\n=== Real NFL Data Integration Demo ===")
    
    try:
        # Load actual 2024 NFL data
        print("Loading 2024 play-by-play data...")
        pbp = nfl.import_pbp_data([2024])
        
        # Filter for regular season
        pbp_reg = pbp[pbp['season_type'] == 'REG']
        print(f"Loaded {len(pbp_reg):,} regular season plays")
        
        # Calculate team EPA stats (simplified version)
        team_stats = pbp_reg[pbp_reg['epa'].notna() & pbp_reg['posteam'].notna()].groupby('posteam').agg({
            'epa': 'mean'
        }).round(4).reset_index()
        team_stats.columns = ['team', 'epa_per_play']
        
        # Filter to reasonable number of teams
        team_stats = team_stats.head(8)  # Just show top 8 for demo
        
        print(f"Sample of team EPA data:")
        print(team_stats.to_string(index=False))
        
        # Create a simple visualization
        fig = nflplot.plot_team_stats(
            pd.DataFrame({
                'team': team_stats['team'].tolist(),
                'epa_per_play': team_stats['epa_per_play'].tolist(),
                'success_rate': np.random.normal(0.45, 0.03, len(team_stats))  # Add some random success rate for demo
            }),
            x='epa_per_play',
            y='success_rate',
            show_logos=False,
            title='Real 2024 NFL Data Demo'
        )
        
        # Save demo plot
        output_dir = os.path.dirname(__file__)
        plt.savefig(os.path.join(output_dir, 'real_data_demo.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print("✓ Created real data demo plot: nflplotpy/examples/real_data_demo.png")
        
    except Exception as e:
        print(f"Error loading real data: {e}")
        print("This is expected if you don't have internet connection or nfl_data_py setup")


def demo_plotly_integration():
    """Demonstrate plotly integration (if plotly is available)."""
    try:
        import plotly.graph_objects as go
        from nflplotpy.plotly.traces import create_team_scatter
        
        print("\n=== Plotly Integration Demo ===")
        
        # Sample data
        teams = ['KC', 'BUF', 'GB', 'TB', 'BAL']
        np.random.seed(456)
        x_data = np.random.normal(0, 0.1, len(teams))
        y_data = np.random.normal(0.45, 0.05, len(teams))
        
        # Create plotly scatter plot
        fig = create_team_scatter(
            teams=teams,
            x=x_data.tolist(),
            y=y_data.tolist(),
            show_logos=False,  # Set to True to test logo integration
            marker_size=20
        )
        
        fig.update_layout(
            title="NFL Teams - Plotly Scatter Plot",
            xaxis_title="EPA per Play", 
            yaxis_title="Success Rate"
        )
        
        # Save as HTML
        fig.write_html('examples/plotly_team_scatter.html')
        print("Saved plotly demo to 'examples/plotly_team_scatter.html'")
        
    except ImportError:
        print("\n=== Plotly Integration Demo ===")
        print("Plotly not available - skipping plotly demo")


def main():
    """Run all demos."""
    print("nflplotpy Demo Script")
    print("====================")
    
    # Create examples directory
    import os
    os.makedirs('examples', exist_ok=True)
    
    # Run demos
    demo_basic_functionality()
    demo_matplotlib_integration()
    demo_high_level_plotting()
    demo_color_palettes()
    demo_with_real_nfl_data()
    demo_plotly_integration()
    
    print("\n=== Demo Complete ===")
    print("Check the 'examples/' directory for generated plots!")


if __name__ == "__main__":
    main()