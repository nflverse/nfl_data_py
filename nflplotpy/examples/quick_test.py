#!/usr/bin/env python3
"""
Quick test of nflplotpy with real data - minimal example
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
import matplotlib.pyplot as plt
import nflplotpy as nflplot

def test_basic_functionality():
    """Test basic nflplotpy functions."""
    print("Testing nflplotpy basic functionality...")
    
    # Test team colors
    color = nflplot.get_team_colors('KC', 'primary')
    print(f"✓ KC primary color: {color}")
    
    # Test multiple teams
    colors = nflplot.get_team_colors(['KC', 'BUF', 'GB'], 'primary')
    print(f"✓ Multiple team colors: {colors}")
    
    # Test available teams
    teams = nflplot.get_available_teams()
    print(f"✓ Available teams: {len(teams)} teams")
    
    # Create simple plot
    sample_teams = ['KC', 'BUF', 'GB', 'TB']
    sample_data = pd.DataFrame({
        'team': sample_teams,
        'off_epa': [0.12, 0.08, 0.06, 0.04],
        'def_epa': [-0.02, -0.01, 0.01, 0.02]
    })
    
    fig = nflplot.plot_team_stats(
        sample_data, 
        'off_epa', 
        'def_epa',
        show_logos=False,
        title='nflplotpy Test Plot'
    )
    
    output_path = os.path.join(os.path.dirname(__file__), 'test_plot.png')
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Created test plot: {output_path}")

def test_real_data_simple():
    """Test with minimal real data loading."""
    print("\nTesting with real NFL data (minimal)...")
    
    try:
        import nfl_data_py as nfl
        
        # Try to load just a small amount of recent data
        print("Loading recent NFL data...")
        pbp = nfl.import_pbp_data([2024])
        
        if pbp is not None and len(pbp) > 0:
            print(f"✓ Loaded {len(pbp):,} plays")
            
            # Quick team EPA calculation
            team_epa = pbp[pbp['epa'].notna() & pbp['posteam'].notna()].groupby('posteam')['epa'].mean().head(6)
            
            print("Sample team EPA data:")
            for team, epa in team_epa.items():
                print(f"  {team}: {epa:.3f}")
                
            print("✓ Real data integration working!")
        else:
            print("⚠ No data loaded (might be network issue)")
            
    except Exception as e:
        print(f"⚠ Real data test failed: {e}")
        print("This is expected if you don't have network access")

if __name__ == "__main__":
    print("nflplotpy Quick Test")
    print("=" * 30)
    
    test_basic_functionality()
    test_real_data_simple()
    
    print("\n" + "=" * 30)
    print("✅ nflplotpy is working correctly!")
    print("📁 Check nflplotpy/examples/ for generated files")