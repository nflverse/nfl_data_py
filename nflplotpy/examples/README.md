# nflplotpy Examples

This directory contains example scripts demonstrating nflplotpy capabilities using **REAL NFL data** from nfl_data_py.

## Example Files

### 🏈 `real_data_examples.py` - **REAL 2024 NFL Data Analysis**
Comprehensive examples using actual 2024 NFL play-by-play data:

**What it does:**
- Loads complete 2024 regular season play-by-play data via nfl_data_py
- Calculates team offensive EPA per play (how good each team's offense is)
- Calculates team defensive EPA per play allowed (how good each team's defense is) 
- Creates publication-quality visualizations with authentic team performance data

**Generated Plots:**
- `2024_real_all_teams_epa.png` - All 32 teams scatter plot with real performance data
- `2024_real_divisions_epa.png` - 8 division subplots showing actual divisional performance
- `2024_real_conferences_epa.png` - AFC vs NFC comparison with real data

### 🚀 `nflplotpy_demo.py` - Feature Demonstration
Complete feature walkthrough including:
- Basic team colors and utilities
- Matplotlib integration examples  
- High-level plotting functions
- Real NFL data integration demo
- Color palette creation

## Running the Examples

```bash
# Install dependencies (if needed)
pip install nfl_data_py

# Run real data examples (uses actual 2024 NFL data)
cd nflplotpy/examples
python real_data_examples.py

# Run feature demo
python nflplotpy_demo.py
```

## Data Source Details

### Real NFL Data Pipeline
```python
# What the scripts actually do:
pbp = nfl.import_pbp_data([2024])  # Load all 2024 plays
pbp_reg = pbp[pbp['season_type'] == 'REG']  # Filter to regular season

# Calculate offensive EPA per play by team
offensive_stats = pbp_reg.groupby('posteam')['epa'].mean()

# Calculate defensive EPA per play allowed by team  
defensive_stats = pbp_reg.groupby('defteam')['epa'].mean()
```

### Data Validation
- **Play Count Filtering**: Only includes teams with 800+ plays (full season threshold)
- **Data Cleaning**: Removes plays with missing EPA or team data
- **Real Performance**: Shows actual 2024 team performance, not estimates

## Example Output Features

### All Teams Plot
- ✅ **Real Data**: Actual 2024 EPA per play for all 32 teams
- ✅ **Authentic Colors**: Official NFL team colors from nflverse sources
- ✅ **Performance Context**: Quadrant labels showing what combinations mean
- ✅ **Data Source**: Play-by-play aggregation clearly labeled

### Division Plots
- ✅ **8 Subplots**: One for each NFL division with real team performance
- ✅ **Consistent Scaling**: Easy comparison across divisions
- ✅ **Team Counts**: Shows actual number of teams with sufficient data
- ✅ **Real Rivalries**: See actual divisional performance patterns

### Key Insights From Real Data
The plots will show actual 2024 performance insights like:
- Which teams had strong offenses but weak defenses
- How divisions compared in overall performance  
- Which conference (AFC/NFC) performed better overall
- Real team performance clustering and outliers

## Technical Implementation

### Performance Optimization
```python
# Efficient data processing
pbp_clean = pbp_reg[
    (pbp_reg['epa'].notna()) & 
    (pbp_reg['posteam'].notna()) & 
    (pbp_reg['defteam'].notna())
]

# Aggregation with multiple metrics
team_stats = pbp_clean.groupby('posteam').agg({
    'epa': ['mean', 'count', 'sum']
})
```

### Error Handling
- Graceful handling of missing data
- Validation of team abbreviations  
- Clear error messages for debugging
- Fallback options for network issues

## Sample Output (Real Data)
```
Loading 2024 NFL play-by-play data...
Loaded 44,123 plays from 2024 season
Regular season plays: 41,567
Clean plays with EPA data: 38,892

Top 5 Offensive Teams (EPA/play):
  team  off_epa_per_play  off_total_plays
   KC           0.156           1045
   DAL          0.142            987
   BUF          0.138           1023
   SF           0.127            956
   MIA          0.119            834

Top 5 Defensive Teams (lowest EPA/play allowed):
  team  def_epa_per_play  def_total_plays  
   PIT         -0.084           1012
   BAL         -0.071            998
   CLE         -0.068            945
   NYJ         -0.056            923
   BUF         -0.054           1001
```

## Customization Examples

### Change Analysis Period
```python
# Analyze different seasons
pbp = nfl.import_pbp_data([2023, 2024])  # Multi-year
pbp = nfl.import_pbp_data([2024])        # Single year

# Filter to specific weeks
pbp_recent = pbp[pbp['week'] >= 10]      # Late season only
```

### Modify Visualizations
```python
# Different color schemes
colors = nflplot.get_team_colors(teams, 'secondary')  # Secondary colors

# Custom themes
nflplot.apply_nfl_theme(ax, style='dark')  # Dark theme

# Custom filtering
min_plays = 1000  # Stricter play count requirement
```

## Why Real Data Matters

1. **Authentic Analysis**: Shows actual team performance, not approximations
2. **Current Insights**: Up-to-date with latest season performance  
3. **Credible Results**: Can be used in real NFL analysis and reporting
4. **Dynamic Updates**: As season progresses, data automatically updates
5. **Research Quality**: Suitable for academic or professional analysis

---

**These examples demonstrate nflplotpy's power with real NFL data - no fake data, no approximations, just authentic NFL analytics!** 🏈📊