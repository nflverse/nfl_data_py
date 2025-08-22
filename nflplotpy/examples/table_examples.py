#!/usr/bin/env python3
"""
nflplotpy Table Examples

Demonstrates the new pandas integration features:
- Team logos in tables
- NFL-themed table styling
- Color-coded team data
"""

import pandas as pd
import numpy as np
import os
import sys

# Import nflplotpy
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
import nflplotpy as nflplot

def create_sample_standings_data():
    """Create sample NFL standings data."""
    return pd.DataFrame({
        'team': ['KC', 'BUF', 'BAL', 'CIN', 'HOU', 'IND', 'JAC', 'TEN'],
        'wins': [14, 13, 13, 9, 10, 9, 4, 6],
        'losses': [3, 4, 4, 8, 7, 8, 13, 11],
        'win_pct': [0.824, 0.765, 0.765, 0.529, 0.588, 0.529, 0.235, 0.353],
        'points_for': [456, 482, 391, 393, 353, 331, 307, 279],
        'points_against': [319, 344, 287, 348, 384, 414, 407, 361]
    })

def create_sample_player_stats():
    """Create sample player statistics data."""
    return pd.DataFrame({
        'player_name': ['Patrick Mahomes', 'Josh Allen', 'Lamar Jackson', 'Joe Burrow'],
        'team': ['KC', 'BUF', 'BAL', 'CIN'],
        'passing_yards': [4183, 4306, 3678, 4056],
        'passing_tds': [27, 29, 24, 35],
        'interceptions': [14, 18, 7, 12],
        'qbr': [65.7, 73.2, 64.8, 67.4]
    })

def example_basic_table_with_logos():
    """Example 1: Basic table with team logos."""
    print("🏈 Example 1: Basic Table with Team Logos")
    print("=" * 50)
    
    # Create sample data
    standings = create_sample_standings_data()
    
    # Style with logos
    styled_table = nflplot.style_with_logos(standings, 'team')
    
    # Save to HTML
    output_path = os.path.join(os.path.dirname(__file__), 'basic_logos_table.html')
    styled_table.to_html(output_path, escape=False)
    
    print(f"✅ Saved basic logos table to: {output_path}")
    print("   Open in browser to view team logos in table!")

def example_comprehensive_nfl_table():
    """Example 2: Comprehensive NFL-themed table.""" 
    print("\n🏈 Example 2: Comprehensive NFL Table")
    print("=" * 50)
    
    # Create sample data
    standings = create_sample_standings_data()
    
    # Create comprehensive NFL table
    table = nflplot.create_nfl_table(
        standings,
        team_column='team',
        logo_columns='team',
        color_columns=['wins', 'losses', 'win_pct'],
        title='2024 AFC Standings'
    )
    
    # Save to HTML
    output_path = os.path.join(os.path.dirname(__file__), 'comprehensive_nfl_table.html')
    table.save_html(output_path)
    
    print(f"✅ Saved comprehensive table to: {output_path}")
    print("   Features: logos, team colors, NFL styling!")

def example_player_stats_table():
    """Example 3: Player statistics table."""
    print("\n🏈 Example 3: Player Statistics Table")  
    print("=" * 50)
    
    # Create sample data
    player_stats = create_sample_player_stats()
    
    # Create styled table
    table = nflplot.NFLTableStyler(player_stats)
    styled = table.with_team_logos('team').with_nfl_theme().with_team_colors(
        ['passing_yards', 'passing_tds'], 'team', apply_to='background'
    )
    
    # Save to HTML
    output_path = os.path.join(os.path.dirname(__file__), 'player_stats_table.html')
    styled.to_html().replace('table', 'table').replace('<th>', '<th style="padding: 10px;">')
    
    with open(output_path, 'w') as f:
        f.write(styled.to_html())
    
    print(f"✅ Saved player stats table to: {output_path}")
    print("   Features: player names, team logos, colored stats!")

def example_division_comparison_table():
    """Example 4: Division comparison table."""
    print("\n🏈 Example 4: Division Comparison")
    print("=" * 50)
    
    # Create division data
    afc_west = pd.DataFrame({
        'team': ['KC', 'DEN', 'LV', 'LAC'],
        'division': ['AFC West'] * 4,
        'wins': [14, 8, 8, 5],
        'losses': [3, 9, 9, 12],
        'playoff_status': ['Division Winner', 'Eliminated', 'Eliminated', 'Eliminated']
    })
    
    # Style the table
    styled = nflplot.style_with_logos(afc_west, 'team')
    
    # Custom CSS for better styling
    html_content = styled.to_html(escape=False)
    
    # Add custom styling
    custom_css = """
    <style>
    table { 
        font-family: Arial, sans-serif; 
        border-collapse: collapse; 
        margin: 20px auto; 
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }
    th { 
        background-color: #013369; 
        color: white; 
        font-weight: bold; 
        padding: 12px; 
        text-align: center;
    }
    td { 
        padding: 10px; 
        text-align: center; 
        border-bottom: 1px solid #ddd;
    }
    tr:nth-child(even) { 
        background-color: #f2f2f2; 
    }
    .playoff-winner { 
        background-color: #28a745 !important; 
        color: white; 
        font-weight: bold;
    }
    </style>
    """
    
    final_html = custom_css + html_content
    
    # Save to HTML
    output_path = os.path.join(os.path.dirname(__file__), 'division_table.html')
    with open(output_path, 'w') as f:
        f.write(final_html)
    
    print(f"✅ Saved division table to: {output_path}")
    print("   Features: division standings with playoff status!")

def example_preview_functionality():
    """Example 5: Demonstrate plot preview functionality."""
    print("\n📊 Example 5: Plot Preview Functionality")
    print("=" * 50)
    
    import matplotlib.pyplot as plt
    
    # Create a simple plot
    fig, ax = plt.subplots()
    
    # Sample data
    teams = ['KC', 'BUF', 'BAL', 'CIN']
    wins = [14, 13, 13, 9]
    
    # Create bar chart with team colors
    colors = nflplot.get_team_colors(teams)
    bars = ax.bar(teams, wins, color=colors)
    
    ax.set_title('AFC Playoff Teams - Wins', fontsize=14, fontweight='bold')
    ax.set_ylabel('Wins')
    
    # Apply NFL theme
    nflplot.apply_nfl_theme(ax)
    
    # Preview the plot
    preview_path = nflplot.nfl_preview(
        fig, 
        width=10, 
        height=6, 
        dpi=150,
        show_in_notebook=False,
        save_path=os.path.join(os.path.dirname(__file__), 'plot_preview.png')
    )
    
    plt.close(fig)
    
    print(f"✅ Saved plot preview to: {preview_path}")
    print("   Demonstrates nfl_preview() functionality!")

def main():
    """Run all table examples."""
    print("🏈 nflplotpy Table Integration Examples")
    print("=" * 60)
    print("Demonstrating new pandas styling and preview features!")
    
    try:
        # Run all examples
        example_basic_table_with_logos()
        example_comprehensive_nfl_table()
        example_player_stats_table()
        example_division_comparison_table()
        example_preview_functionality()
        
        print("\n" + "=" * 60)
        print("✅ All examples completed successfully!")
        print("\nGenerated files:")
        print("- basic_logos_table.html")
        print("- comprehensive_nfl_table.html")
        print("- player_stats_table.html")
        print("- division_table.html")
        print("- plot_preview.png")
        
        print("\n📋 Features demonstrated:")
        print("✓ Team logos in pandas tables")
        print("✓ NFL-themed table styling")
        print("✓ Team color integration")
        print("✓ Plot preview functionality")
        print("✓ Comprehensive table creation")
        
        print(f"\n🌟 These examples show nflplotpy's new features!")
        print("   Open the HTML files in your browser to see the results!")
        
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()