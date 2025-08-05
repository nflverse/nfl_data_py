# nflplotpy

**Python NFL Visualization Package - nflplotR Equivalent**

nflplotpy is a comprehensive Python visualization package for NFL data, providing 1:1 feature parity with R's popular `nflplotR` package. It's designed to integrate seamlessly with `nfl_data_py` and the broader nflverse ecosystem.

## Quick Start

```python
import nfl_data_py as nfl
import nflplotpy as nflplot

# Load NFL data
pbp = nfl.import_pbp_data([2023])

# Create team performance visualization
fig = nflplot.plot_team_stats(
    team_data,
    x='epa_per_play',
    y='success_rate', 
    show_logos=True,
    title='2023 NFL Team Performance'
)
```

## Core Features

### 🎨 **Team Colors & Branding**
```python
# Get team colors
nflplot.get_team_colors('KC', 'primary')  # '#e31837'
nflplot.get_team_colors(['KC', 'BUF', 'GB'])  # Multiple teams

# Create color palettes
palette = nflplot.NFLColorPalette()
afc_colors = palette.create_conference_palette('AFC')
gradient = palette.create_gradient('KC', 'SF', n_colors=10)
```

### 🏈 **NFL Elements**
```python
import matplotlib.pyplot as plt

fig, ax = plt.subplots()

# Add team logos
nflplot.add_nfl_logo(ax, 'KC', x=0.5, y=0.5, width=0.1)

# Add reference lines
nflplot.add_median_lines(ax, data, axis='both')

# Apply NFL theme
nflplot.apply_nfl_theme(ax, team='KC', style='default')
```

### 📊 **High-Level Plotting**
```python
# Team performance scatter plot
fig = nflplot.plot_team_stats(
    data, x='offensive_epa', y='defensive_epa',
    show_logos=True, add_reference_lines=True
)

# Player comparison radar chart
fig = nflplot.plot_player_comparison(
    player_data, players=['Josh Allen', 'Patrick Mahomes'],
    metrics=['passing_yards', 'passing_tds', 'qbr'],
    plot_type='radar'
)
```

## API Reference

### Main Functions

| Function | Description | nflplotR Equivalent |
|----------|-------------|-------------------|
| `get_team_colors()` | Get NFL team colors | `team_colors` |
| `add_nfl_logo()` | Add team logo to plot | `geom_nfl_logos()` |
| `add_median_lines()` | Add reference lines | `geom_median_lines()` |
| `plot_team_stats()` | High-level team plots | Custom implementation |
| `team_factor()` | Ordered team factors | `nfl_team_factor()` |
| `team_tiers()` | Group teams by tiers | `nfl_team_tiers()` |

### Classes

- **`NFLAssetManager`**: Manages logo caching and asset downloads
- **`NFLColorPalette`**: Advanced color palette management

### Visualization Backends

- **Matplotlib**: `nflplotpy.matplotlib.*`
- **Plotly**: `nflplotpy.plotly.*` 
- **Seaborn**: `nflplotpy.seaborn.*`

## Installation

nflplotpy is included with nfl_data_py:

```bash
pip install nfl_data_py  # Basic installation
pip install nfl_data_py[nflplotpy]  # With plotly support
```

## Examples

See `examples/nflplotpy_demo.py` for comprehensive usage examples.

## Package Structure

```
nflplotpy/
├── core/           # Core functionality (colors, logos, utilities)
├── matplotlib/     # Matplotlib integration
├── plotly/         # Plotly integration  
├── seaborn/        # Seaborn integration
├── data/           # Team metadata
└── tests/          # Test suite
```

## Contributing

Part of the nflverse ecosystem. Contributions welcome!

## License

MIT License (same as nfl_data_py)

---

**Built for the NFL analytics community** 🏈