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

### 📋 **NFL Tables (NEW!)**
```python
# Style pandas DataFrames with team logos
styled = nflplot.style_with_logos(df, 'team')
styled.to_html('nfl_table.html')

# Comprehensive NFL-themed tables
table = nflplot.create_nfl_table(
    standings_df, 
    team_column='team',
    title='2024 NFL Standings'
)
table.save_html('standings.html')
```

### 🔍 **Plot Preview**
```python
# Preview plots with specified dimensions
nflplot.nfl_preview(fig, width=12, height=8, dpi=150)

# Quick preview with presets
nflplot.preview_with_dimensions(fig, 'presentation')  # 16:9 format
```

### 🏷️ **Advanced Elements**
```python
# Add logos to axis labels
nflplot.set_xlabel_with_logos(ax, ['KC', 'BUF', 'NE', 'NYJ'])

# Add logo watermarks
nflplot.add_logo_watermark(ax, 'KC', position='bottom_right')

# Create team comparison layouts
left_ax, right_ax = nflplot.create_team_comparison_axes(fig, 'KC', 'BUF')
```

## API Reference

### Main Functions

| Function | Description | nflplotR Equivalent |
|----------|-------------|-------------------|
| `get_team_colors()` | Get NFL team colors | `team_colors` |
| `add_nfl_logo()` | Add team logo to plot | `geom_nfl_logos()` |
| `add_median_lines()` | Add reference lines | `geom_median_lines()` |
| `style_with_logos()` | Add logos to tables | `gt_nfl_logos()` |
| `nfl_preview()` | Preview plots | `ggpreview()` |
| `nfl_sitrep()` | System information | `nflverse_sitrep` |
| `plot_team_stats()` | High-level team plots | Custom implementation |
| `team_factor()` | Ordered team factors | `nfl_team_factor()` |
| `team_tiers()` | Group teams by tiers | `nfl_team_tiers()` |

### Classes

- **`NFLAssetManager`**: Manages logo caching and asset downloads
- **`NFLColorPalette`**: Advanced color palette management
- **`NFLTableStyler`**: Pandas DataFrame styling with NFL elements
- **`AssetURLManager`**: Comprehensive URL management for all NFL assets

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

See `nflplotpy/examples/` for comprehensive usage examples:
- `real_data_examples.py`: Complete 2024 NFL analysis using real play-by-play data
- `nflplotpy_demo.py`: Feature demonstrations and tutorials
- `quick_test.py`: Simple functionality test

## Package Structure

```
nflplotpy/
├── core/           # Core functionality (colors, logos, utilities, URLs)
├── matplotlib/     # Matplotlib integration (artists, scales, preview, elements)
├── plotly/         # Plotly integration  
├── seaborn/        # Seaborn integration
├── pandas/         # Pandas table styling integration
├── data/           # Team metadata
├── examples/       # Usage examples and tutorials
└── tests/          # Comprehensive test suite
```

## Contributing

Part of the nflverse ecosystem. Contributions welcome!

## License

MIT License (same as nfl_data_py)

---

**Built for the NFL analytics community** 🏈