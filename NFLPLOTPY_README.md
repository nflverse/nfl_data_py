# nflplotpy - Python NFL Visualization Package

A comprehensive Python visualization package for NFL data, providing the Python equivalent of R's popular `nflplotR` package. Built to integrate seamlessly with `nfl_data_py` and the broader nflverse ecosystem.

## Features

nflplotpy provides 1:1 feature parity with nflplotR, including:

### 🎨 **Team Branding & Colors**
- Official NFL team colors (primary, secondary, tertiary, quaternary)
- Team logo integration from official sources
- Conference and division color palettes
- Automatic color scaling and gradients

### 📊 **Visualization Backends**
- **Matplotlib**: Full integration with custom NFL themes and styling
- **Plotly**: Interactive visualizations with team branding
- **Seaborn**: Statistical plotting with NFL color palettes

### 🏈 **NFL-Specific Elements**
- Team logos as plot elements
- Player headshots (coming soon)
- Team wordmarks (coming soon)  
- Reference lines (median, mean)
- NFL-themed plot styling

### 📈 **High-Level Plotting Functions**
- `plot_team_stats()`: Team performance scatter plots with logos/colors
- `plot_player_comparison()`: Radar and bar charts for player analysis
- `plot_game_flow()`: Game flow visualizations (coming soon)
- `plot_season_standings()`: Standings tables and charts (coming soon)

## Installation

nflplotpy is included with `nfl_data_py`. To install with full visualization support:

```bash
# Basic installation (matplotlib support)
pip install nfl_data_py

# Full installation with all visualization backends
pip install nfl_data_py[nflplotpy]
```

## Quick Start

```python
import pandas as pd
import nfl_data_py as nfl
import nflplotpy as nflplot

# Load NFL data
pbp = nfl.import_pbp_data([2023])

# Create team stats
team_stats = pbp.groupby('posteam').agg({
    'epa': 'mean',
    'success': 'mean'
}).reset_index()
team_stats.columns = ['team', 'epa_per_play', 'success_rate']

# Create visualization with team logos and colors
fig = nflplot.plot_team_stats(
    team_stats,
    x='epa_per_play',
    y='success_rate',
    show_logos=True,
    title='2023 NFL Team Performance'
)
```

## Core API

### Team Colors
```python
# Get single team color
color = nflplot.get_team_colors('KC', 'primary')

# Get multiple team colors  
colors = nflplot.get_team_colors(['KC', 'BUF', 'GB'], 'primary')

# Create color palette
palette = nflplot.NFLColorPalette()
afc_colors = palette.create_conference_palette('AFC')
```

### Matplotlib Integration
```python
import matplotlib.pyplot as plt

fig, ax = plt.subplots()

# Add team logos to plot
nflplot.add_nfl_logo(ax, 'KC', x=0.5, y=0.5, width=0.1)

# Apply NFL theme
nflplot.apply_nfl_theme(ax, team='KC', style='default')

# Add reference lines
nflplot.add_median_lines(ax, data, axis='both')
```

### Plotly Integration
```python
from nflplotpy.plotly import create_team_scatter

# Interactive scatter plot with team colors
fig = create_team_scatter(
    teams=['KC', 'BUF', 'GB'],
    x=[0.1, 0.05, 0.08], 
    y=[0.48, 0.45, 0.47],
    show_logos=True
)
```

## nflplotR Function Equivalents

| nflplotR (R) | nflplotpy (Python) | Description |
|--------------|-------------------|-------------|
| `geom_nfl_logos()` | `add_nfl_logo()` | Add team logos to plots |
| `geom_nfl_headshots()` | `add_nfl_headshot()` | Add player headshots |
| `geom_median_lines()` | `add_median_lines()` | Add median reference lines |
| `scale_color_nfl()` | `nfl_color_scale()` | NFL team color scales |
| `nfl_team_factor()` | `team_factor()` | Create ordered team factors |
| `nfl_team_tiers()` | `team_tiers()` | Group teams into tiers |

## Advanced Usage

### Custom Color Palettes
```python
palette = nflplot.NFLColorPalette()

# Create division palette
nfc_west = palette.create_division_palette('NFC West')

# Create gradient between teams
gradient = palette.create_gradient('KC', 'SF', n_colors=10)

# Matplotlib colormap
cmap = palette.to_matplotlib_colormap(['KC', 'BUF', 'GB'])
```

### Asset Management
```python
# Manage logo/asset caching
manager = nflplot.NFLAssetManager()

# Get cache information
info = manager.get_cache_info()

# Clear cache if needed
manager.clear_cache('logos')
```

### Team Utilities
```python
# Validate team abbreviations
teams = nflplot.validate_teams(['KC', 'buf', 'GB'])  # ['KC', 'BUF', 'GB']

# Get comprehensive team info
info = nflplot.get_team_info(['KC', 'BUF'])

# Clean team abbreviations in DataFrame
df = nflplot.clean_team_abbreviations(df, 'team_colum')
```

## Data Sources

nflplotpy uses the same data sources as nflplotR:
- **Team Logos**: Wikipedia, NFL.com, ESPN
- **Team Colors**: Lee Sharpe's nfldata repository
- **Player Data**: Integration with nfl_data_py
- **Team Info**: nflverse data ecosystem

## Examples

See the `examples/` directory for comprehensive examples:
- `nflplotpy_demo.py`: Complete functionality demonstration
- Team performance scatter plots
- Player comparison radar charts
- Interactive plotly visualizations

## Testing

```bash
# Run all tests
pytest nflplotpy/tests/

# Run specific test files
pytest nflplotpy/tests/test_core.py
pytest nflplotpy/tests/test_matplotlib.py
```

## Contributing

nflplotpy is part of the nflverse ecosystem. Contributions welcome!

1. Fork the repository
2. Create feature branch
3. Add tests for new functionality  
4. Ensure all tests pass
5. Submit pull request

## Relationship to nfl_data_py

nflplotpy is designed as a companion to `nfl_data_py`:
- **nfl_data_py**: Data loading and processing
- **nflplotpy**: Visualization and plotting

Together they provide a complete Python solution for NFL analytics, equivalent to the R nflverse ecosystem (`nflfastR` + `nflplotR`).

## License

MIT License - same as nfl_data_py

## Acknowledgments

- **nflplotR**: Original R package that inspired this Python implementation
- **nflverse**: Community and data sources
- **Lee Sharpe**: Team colors and logos data
- **nfl_data_py**: Data foundation

---

*Built with ❤️ for the NFL analytics community*