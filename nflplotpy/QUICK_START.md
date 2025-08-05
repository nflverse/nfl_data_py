# nflplotpy Quick Start Guide

Get up and running with NFL visualizations in 5 minutes!

## Installation

```bash
pip install nfl_data_py  # Includes nflplotpy
```

## Basic Usage

### 1. Import the packages

```python
import nfl_data_py as nfl
import nflplotpy as nflplot
import matplotlib.pyplot as plt
import pandas as pd
```

### 2. Get team colors

```python
# Single team
kc_color = nflplot.get_team_colors('KC', 'primary')
print(kc_color)  # '#e31837'

# Multiple teams
colors = nflplot.get_team_colors(['KC', 'BUF', 'GB'], 'primary')
print(colors)  # ['#e31837', '#00338d', '#203731']
```

### 3. Create a simple scatter plot with team colors

```python
# Sample data
teams = ['KC', 'BUF', 'GB', 'TB', 'BAL', 'SEA']
epa_data = [0.12, 0.08, 0.06, 0.04, 0.10, 0.02]
success_data = [0.48, 0.46, 0.45, 0.44, 0.47, 0.43]

# Get team colors
colors = nflplot.get_team_colors(teams, 'primary')

# Create plot
fig, ax = plt.subplots(figsize=(10, 6))
ax.scatter(epa_data, success_data, c=colors, s=150, alpha=0.8, edgecolors='white', linewidth=2)

# Add team labels
for i, team in enumerate(teams):
    ax.annotate(team, (epa_data[i], success_data[i]), 
                textcoords='offset points', xytext=(5,5), fontweight='bold')

# Style the plot
nflplot.apply_nfl_theme(ax, style='default')
ax.set_xlabel('EPA per Play')
ax.set_ylabel('Success Rate')
ax.set_title('NFL Team Performance')

plt.tight_layout()
plt.show()
```

### 4. Use high-level plotting functions

```python
# Create DataFrame
df = pd.DataFrame({
    'team': teams,
    'epa_per_play': epa_data,
    'success_rate': success_data
})

# High-level plot with reference lines
fig = nflplot.plot_team_stats(
    df, 
    x='epa_per_play', 
    y='success_rate',
    show_logos=False,  # Set to True to show team logos
    add_reference_lines=True,
    reference_type='median',
    title='NFL Team Performance - High Level API'
)

plt.show()
```

### 5. Work with real NFL data

```python
# Load real data (if you have nfl_data_py data)
try:
    # Load play-by-play data
    pbp = nfl.import_pbp_data([2023])
    
    # Calculate team stats
    team_stats = pbp.groupby('posteam').agg({
        'epa': 'mean',
        'success': 'mean'
    }).reset_index()
    team_stats.columns = ['team', 'epa_per_play', 'success_rate']
    
    # Remove NaN teams
    team_stats = team_stats[team_stats['team'].notna()]
    
    # Create visualization
    fig = nflplot.plot_team_stats(
        team_stats,
        x='epa_per_play',
        y='success_rate', 
        title='2023 NFL Team Performance (Real Data)'
    )
    
    plt.show()
    
except Exception as e:
    print(f"Error loading real data: {e}")
    print("Using sample data instead...")
```

## Advanced Features

### Color Palettes

```python
# Create palette manager
palette = nflplot.NFLColorPalette()

# Conference colors
afc_colors = palette.create_conference_palette('AFC')
nfc_colors = palette.create_conference_palette('NFC')

# Division colors
nfc_west = palette.create_division_palette('NFC West')

# Color gradients
kc_sf_gradient = palette.create_gradient('KC', 'SF', n_colors=10)
```

### Reference Lines

```python
fig, ax = plt.subplots()

# Your scatter plot here...

# Add reference lines
nflplot.add_median_lines(ax, epa_data, axis='x')
nflplot.add_mean_lines(ax, success_data, axis='y')
```

### Team Utilities

```python
# Available teams
all_teams = nflplot.get_available_teams()
print(f"Total teams: {len(all_teams)}")

# Team tiers
tiers = nflplot.team_tiers('conference')
print("AFC teams:", len(tiers['AFC']))
print("NFC teams:", len(tiers['NFC']))

# Team factors (for ordered categorical data)
factor = nflplot.team_factor(['KC', 'BUF', 'GB'])
```

## Next Steps

- Check out `nflplotpy/examples/real_data_examples.py` for real NFL data analysis
- Try `nflplotpy/examples/nflplotpy_demo.py` for comprehensive feature examples
- Read the full documentation in `nflplotpy/README.md`
- Explore the API reference for advanced features
- Try integrating with your existing NFL analysis workflows!

## Common Issues

1. **Import Error**: Make sure you have the latest version of nfl_data_py
2. **Missing Dependencies**: Install with `pip install matplotlib pillow requests`
3. **Team Abbreviations**: Use standard NFL team abbreviations (KC, BUF, GB, etc.)

Happy plotting! 🏈📊