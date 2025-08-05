# nflplotpy Examples

This directory contains example scripts demonstrating nflplotpy capabilities.

## Example Files

### 📊 `example_plots_2024.py`
Comprehensive examples showcasing 2024 NFL season visualizations:
- **All Teams Plot**: Offensive vs Defensive EPA for all 32 teams
- **Division Subplots**: 8-panel visualization showing each division separately  
- **Conference Comparison**: AFC vs NFC side-by-side analysis

**Generated Plots:**
- `2024_all_teams_epa.png` - All 32 teams scatter plot
- `2024_divisions_epa.png` - 8 division subplots
- `2024_conferences_epa.png` - AFC vs NFC comparison

### 🚀 `nflplotpy_demo.py`
Complete feature demonstration including:
- Basic team colors and utilities
- Matplotlib integration examples
- High-level plotting functions
- Color palette creation
- Asset management

## Running the Examples

```bash
# Install dependencies
pip install nfl_data_py

# Run 2024 NFL examples
python examples/example_plots_2024.py

# Run full feature demo
python examples/nflplotpy_demo.py
```

## Generated Plots

The example scripts create high-quality visualizations showcasing:

### All Teams Plot Features
- ✅ Official NFL team colors for all 32 teams
- ✅ Professional scatter plot styling
- ✅ Team abbreviation labels with contrasting backgrounds
- ✅ Reference lines at EPA = 0 
- ✅ Quadrant labels explaining good/bad performance
- ✅ NFL-themed styling and typography

### Division Plots Features  
- ✅ 8 subplots (2x4 grid) for each NFL division
- ✅ Consistent axis scaling for easy comparison
- ✅ Division-specific team colors
- ✅ Clean, minimal styling optimized for multiple plots
- ✅ Professional layout with clear division labels

### Key Visualization Elements
- **Team Colors**: Authentic NFL primary colors from official sources
- **Typography**: Bold, readable fonts with proper contrast
- **Layout**: Professional spacing and alignment
- **Reference Lines**: Clear visual guides at meaningful thresholds
- **Annotations**: Team labels with background boxes for readability

## Data Sources

Examples use both real and sample data:
- **Real Data**: Uses `nfl_data_py` to load 2024 play-by-play data when available
- **Sample Data**: Realistic synthetic data based on 2024 season performance
- **Graceful Fallback**: Automatically switches to sample data if real data unavailable

## Customization

All examples can be easily customized:

```python
# Change colors
colors = nflplot.get_team_colors(teams, 'secondary')  # Use secondary colors

# Apply different themes
nflplot.apply_nfl_theme(ax, style='dark')  # Dark theme
nflplot.apply_nfl_theme(ax, team='KC')     # Team-specific theme

# Modify plot styling
fig, ax = plt.subplots(figsize=(16, 12))   # Larger figure
ax.set_title('Custom Title', fontsize=20)  # Custom title
```

## Output Quality

All plots are generated with:
- **High DPI**: 300 DPI for publication-quality output
- **Vector Graphics**: Clean scaling at any size
- **Professional Colors**: Official NFL team color palette
- **Publication Ready**: Suitable for reports, presentations, and publications

---

**These examples demonstrate the power of nflplotpy for creating publication-quality NFL data visualizations!** 🏈📊