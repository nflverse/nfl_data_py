#!/usr/bin/env python3
"""
nflplotpy Demo Script

This script demonstrates the key features of nflplotpy, the Python equivalent
of R's nflplotR package for NFL data visualization.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Import nflplotpy
import nflplotpy as nflplot

# Import nfl_data_py for data
try:
    import nfl_data_py as nfl
    has_nfl_data = True
except ImportError:
    print("nfl_data_py not available, using sample data")
    has_nfl_data = False


def demo_basic_functionality():
    """Demonstrate basic nflplotpy functionality."""
    print("=== nflplotpy Basic Functionality Demo ===")
    
    # 1. Team colors
    print("\n1. Getting team colors:")
    ari_color = nflplot.get_team_colors('ARI', 'primary')
    print(f"Arizona Cardinals primary color: {ari_color}")
    
    multiple_colors = nflplot.get_team_colors(['ARI', 'ATL', 'BAL'], 'primary')
    print(f"Multiple team colors: {multiple_colors}")
    
    # 2. Team utilities
    print("\n2. Team utilities:")
    teams = ['ARI', 'ATL', 'BAL', 'BUF']
    factor = nflplot.team_factor(teams)
    print(f"Team factor: {factor}")
    
    tiers = nflplot.team_tiers('conference')
    print(f"Conference tiers: {list(tiers.keys())}")
    
    # 3. Asset manager
    print("\n3. Asset management:")
    manager = nflplot.NFLAssetManager()
    cache_info = manager.get_cache_info()
    print(f"Cache info: {cache_info}")


def demo_matplotlib_integration():
    """Demonstrate matplotlib integration."""
    print("\n=== Matplotlib Integration Demo ===")
    
    # Create sample team data
    teams = ['ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE']
    np.random.seed(42)
    
    sample_data = pd.DataFrame({
        'team': teams,
        'epa_per_play': np.random.normal(0, 0.1, len(teams)),
        'success_rate': np.random.normal(0.45, 0.05, len(teams)),
        'points_per_game': np.random.normal(22, 5, len(teams))
    })
    
    print("\nSample team data:")
    print(sample_data.head())
    
    # 1. Basic scatter plot with team colors
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Get team colors
    colors = nflplot.get_team_colors(sample_data['team'].tolist(), 'primary')
    
    # Create scatter plot
    scatter = ax.scatter(
        sample_data['epa_per_play'], 
        sample_data['success_rate'],
        c=colors, s=150, alpha=0.8, edgecolors='white', linewidth=2
    )
    
    # Add reference lines
    nflplot.add_median_lines(ax, sample_data['epa_per_play'].values, axis='x', alpha=0.5)
    nflplot.add_median_lines(ax, sample_data['success_rate'].values, axis='y', alpha=0.5)
    
    # Apply NFL theme
    nflplot.apply_nfl_theme(ax, style='default')
    
    # Labels and title
    ax.set_xlabel('EPA per Play', fontsize=12)
    ax.set_ylabel('Success Rate', fontsize=12)
    ax.set_title('NFL Team Performance Comparison', fontsize=16, fontweight='bold')
    
    # Add team labels
    for i, team in enumerate(sample_data['team']):
        ax.annotate(team, 
                   (sample_data['epa_per_play'].iloc[i], sample_data['success_rate'].iloc[i]),
                   xytext=(5, 5), textcoords='offset points', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('examples/team_performance_scatter.png', dpi=300, bbox_inches='tight')
    print("Saved matplotlib demo plot to 'examples/team_performance_scatter.png'")
    plt.close()


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
    
    # Use high-level plotting function
    fig = nflplot.plot_team_stats(
        team_stats,
        x='offensive_epa',
        y='defensive_epa', 
        backend='matplotlib',
        show_logos=False,  # Set to True if you want to test logo loading
        add_reference_lines=True,
        reference_type='both',
        title='Team Offensive vs Defensive EPA',
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
    """Demonstrate with real NFL data if available."""
    if not has_nfl_data:
        print("\n=== Real NFL Data Demo ===")
        print("nfl_data_py not available - skipping real data demo")
        return
        
    print("\n=== Real NFL Data Integration Demo ===")
    
    try:
        # Load some real NFL data
        print("Loading 2023 team stats...")
        
        # This is a placeholder - actual implementation would depend on
        # what specific data is available in nfl_data_py
        print("Real data integration would be implemented here")
        print("Example: pbp_data = nfl.import_pbp_data([2023])")
        print("Then: nflplot.plot_team_stats(processed_data, x='epa', y='success_rate')")
        
    except Exception as e:
        print(f"Error loading real data: {e}")


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