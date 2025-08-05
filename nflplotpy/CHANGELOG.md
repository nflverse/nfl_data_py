# nflplotpy Changelog

All notable changes to the nflplotpy package will be documented in this file.

## [0.1.0] - 2025-08-05

### Added - Initial Release
- **Core functionality**: Complete implementation of nflplotR equivalent for Python
- **Team colors**: All 32 NFL teams with primary, secondary, tertiary, and quaternary colors
- **Team logos**: Asset management system with caching for team logos
- **Matplotlib integration**: 
  - Custom artists for adding NFL logos to plots
  - NFL-themed styling and color scales  
  - Reference lines (median, mean)
  - Team color legends
- **Plotly integration**:
  - Interactive scatter plots with team colors
  - Team logo traces for interactive plots
  - NFL-themed layouts and styling
- **Seaborn integration**: Custom color palettes and themes
- **High-level plotting functions**:
  - `plot_team_stats()`: Team performance visualizations
  - `plot_player_comparison()`: Player comparison charts (radar, bar)
- **Utility functions**:
  - `team_factor()`: Ordered categorical team data
  - `team_tiers()`: Team grouping by conference, division, etc.
  - Team abbreviation validation and normalization
- **Asset management**: Intelligent caching system for logos and graphics
- **Comprehensive test suite**: Unit tests for all major functionality
- **Documentation**: Complete README, quick start guide, and examples

### API Functions (1:1 nflplotR parity)
- `get_team_colors()` ↔ `scale_color_nfl()`
- `add_nfl_logo()` ↔ `geom_nfl_logos()`
- `add_median_lines()` ↔ `geom_median_lines()`
- `team_factor()` ↔ `nfl_team_factor()`
- `team_tiers()` ↔ `nfl_team_tiers()`
- `nfl_color_scale()` ↔ `scale_color_nfl()`

### Dependencies
- matplotlib >= 3.5.0
- pillow >= 8.0.0  
- requests >= 2.25.0
- pandas >= 1.3.0 (inherited from nfl_data_py)
- numpy >= 1.20.0 (inherited from nfl_data_py)

### Integration
- Seamless integration with nfl_data_py ecosystem
- Compatible with existing NFL data workflows
- Added to main nfl_data_py package as optional visualization component

---

## Future Releases (Planned)

### [0.2.0] - Future
- Player headshot integration
- Team wordmark support
- Enhanced plotly logo rendering
- Additional statistical plotting functions
- Performance optimizations

### [0.3.0] - Future  
- Game flow visualizations
- Season standings plots
- Advanced interactive dashboards
- Additional backend support (bokeh, altair)

---


*This package provides the Python NFL analytics community with the same powerful visualization capabilities that R users have enjoyed with nflplotR.* 🏈
