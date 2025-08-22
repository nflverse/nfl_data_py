"""Utility functions for nflplotpy."""

from typing import List, Optional, Union, Dict, Any
import pandas as pd
import numpy as np
from .logos import get_available_teams, get_conference_teams


def team_factor(teams: Union[List[str], pd.Series], 
               levels: Optional[List[str]] = None) -> pd.Categorical:
    """Create categorical factor for NFL teams with proper ordering.
    
    Equivalent to nflplotR's nfl_team_factor().
    
    Args:
        teams: List or Series of team abbreviations
        levels: Custom ordering for teams. If None, uses alphabetical.
        
    Returns:
        pandas Categorical with proper team ordering
        
    Raises:
        ValueError: If any team abbreviation is invalid
    """
    if isinstance(teams, pd.Series):
        teams = teams.tolist()
    
    # Validate all teams
    available_teams = get_available_teams() + ['AFC', 'NFC', 'NFL']
    invalid_teams = [t for t in teams if t.upper() not in available_teams]
    if invalid_teams:
        raise ValueError(f"Invalid team abbreviations: {invalid_teams}")
    
    # Normalize to uppercase
    teams = [t.upper() for t in teams]
    
    # Set levels (categories)
    if levels is None:
        # Default alphabetical ordering of all NFL teams
        levels = sorted(available_teams)
    else:
        levels = [t.upper() for t in levels]
    
    return pd.Categorical(teams, categories=levels, ordered=True)


def team_tiers(method: str = "draft_order", 
              season: Optional[int] = None) -> Dict[str, List[str]]:
    """Create NFL team tiers based on various ranking methods.
    
    Equivalent to nflplotR's nfl_team_tiers().
    
    Args:
        method: Method for creating tiers ('draft_order', 'conference', 'division', 'random')
        season: Season year for data-based methods (not used in basic implementation)
        
    Returns:
        Dictionary mapping tier names to lists of team abbreviations
        
    Raises:
        ValueError: If method is not supported
    """
    available_teams = get_available_teams()
    
    if method == "draft_order":
        # Reverse draft order (best teams first)
        # This is a simplified implementation - real version would use actual standings
        np.random.seed(42)  # For reproducible "random" draft order
        shuffled_teams = np.random.permutation(available_teams).tolist()
        
        n_teams = len(shuffled_teams)
        tier_size = n_teams // 4
        
        return {
            "Tier 1": shuffled_teams[:tier_size],
            "Tier 2": shuffled_teams[tier_size:2*tier_size],
            "Tier 3": shuffled_teams[2*tier_size:3*tier_size],
            "Tier 4": shuffled_teams[3*tier_size:]
        }
    
    elif method == "conference":
        return {
            "AFC": get_conference_teams("AFC"),
            "NFC": get_conference_teams("NFC")
        }
    
    elif method == "division":
        return {
            "AFC East": ['BUF', 'MIA', 'NE', 'NYJ'],
            "AFC North": ['BAL', 'CIN', 'CLE', 'PIT'],
            "AFC South": ['HOU', 'IND', 'JAC', 'TEN'],
            "AFC West": ['DEN', 'KC', 'LV', 'LAC'],
            "NFC East": ['DAL', 'NYG', 'PHI', 'WAS'],
            "NFC North": ['CHI', 'DET', 'GB', 'MIN'],
            "NFC South": ['ATL', 'CAR', 'NO', 'TB'],
            "NFC West": ['ARI', 'LAR', 'SEA', 'SF']
        }
    
    elif method == "random":
        np.random.shuffle(available_teams)
        n_teams = len(available_teams)
        tier_size = n_teams // 4
        
        return {
            "Random Tier 1": available_teams[:tier_size],
            "Random Tier 2": available_teams[tier_size:2*tier_size],
            "Random Tier 3": available_teams[2*tier_size:3*tier_size],
            "Random Tier 4": available_teams[3*tier_size:]
        }
    
    else:
        raise ValueError(f"Unsupported method: {method}")


def validate_teams(teams: Union[str, List[str]], 
                  allow_conferences: bool = True) -> List[str]:
    """Validate and normalize team abbreviations.
    
    Args:
        teams: Team abbreviation(s) to validate
        allow_conferences: Whether to allow 'AFC', 'NFC', 'NFL'
        
    Returns:
        List of normalized (uppercase) team abbreviations
        
    Raises:
        ValueError: If any team abbreviation is invalid
    """
    if isinstance(teams, str):
        teams = [teams]
    
    # Normalize to uppercase
    teams = [t.upper() for t in teams]
    
    # Get valid teams
    valid_teams = get_available_teams()
    if allow_conferences:
        valid_teams.extend(['AFC', 'NFC', 'NFL'])
    
    # Check for invalid teams
    invalid_teams = [t for t in teams if t not in valid_teams]
    if invalid_teams:
        raise ValueError(f"Invalid team abbreviations: {invalid_teams}")
    
    return teams


def get_team_info(teams: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
    """Get comprehensive team information.
    
    Args:
        teams: Specific teams to get info for. If None, returns all teams.
        
    Returns:
        DataFrame with team information including colors, conference, division
    """
    from .colors import NFL_TEAM_COLORS
    
    if teams is None:
        teams = get_available_teams()
    else:
        teams = validate_teams(teams, allow_conferences=False)
    
    # Create team info DataFrame
    team_data = []
    
    # Simplified division mappings
    divisions = {
        'BUF': 'AFC East', 'MIA': 'AFC East', 'NE': 'AFC East', 'NYJ': 'AFC East',
        'BAL': 'AFC North', 'CIN': 'AFC North', 'CLE': 'AFC North', 'PIT': 'AFC North',
        'HOU': 'AFC South', 'IND': 'AFC South', 'JAC': 'AFC South', 'TEN': 'AFC South',
        'DEN': 'AFC West', 'KC': 'AFC West', 'LV': 'AFC West', 'LAC': 'AFC West',
        'DAL': 'NFC East', 'NYG': 'NFC East', 'PHI': 'NFC East', 'WAS': 'NFC East',
        'CHI': 'NFC North', 'DET': 'NFC North', 'GB': 'NFC North', 'MIN': 'NFC North',
        'ATL': 'NFC South', 'CAR': 'NFC South', 'NO': 'NFC South', 'TB': 'NFC South',
        'ARI': 'NFC West', 'LAR': 'NFC West', 'SEA': 'NFC West', 'SF': 'NFC West'
    }
    
    # Team full names
    team_names = {
        'ARI': 'Arizona Cardinals', 'ATL': 'Atlanta Falcons', 'BAL': 'Baltimore Ravens',
        'BUF': 'Buffalo Bills', 'CAR': 'Carolina Panthers', 'CHI': 'Chicago Bears',
        'CIN': 'Cincinnati Bengals', 'CLE': 'Cleveland Browns', 'DAL': 'Dallas Cowboys',
        'DEN': 'Denver Broncos', 'DET': 'Detroit Lions', 'GB': 'Green Bay Packers',
        'HOU': 'Houston Texans', 'IND': 'Indianapolis Colts', 'JAC': 'Jacksonville Jaguars',
        'KC': 'Kansas City Chiefs', 'LV': 'Las Vegas Raiders', 'LAC': 'Los Angeles Chargers',
        'LAR': 'Los Angeles Rams', 'MIA': 'Miami Dolphins', 'MIN': 'Minnesota Vikings',
        'NE': 'New England Patriots', 'NO': 'New Orleans Saints', 'NYG': 'New York Giants',
        'NYJ': 'New York Jets', 'PHI': 'Philadelphia Eagles', 'PIT': 'Pittsburgh Steelers',
        'SEA': 'Seattle Seahawks', 'SF': 'San Francisco 49ers', 'TB': 'Tampa Bay Buccaneers',
        'TEN': 'Tennessee Titans', 'WAS': 'Washington Commanders'
    }
    
    for team in teams:
        division = divisions.get(team, 'Unknown')
        conference = division.split()[0] if division != 'Unknown' else 'Unknown'
        
        team_data.append({
            'team_abbr': team,
            'team_name': team_names.get(team, f'{team} Team'),
            'conference': conference,
            'division': division,
            'primary_color': NFL_TEAM_COLORS[team]['primary'],
            'secondary_color': NFL_TEAM_COLORS[team]['secondary'],
            'tertiary_color': NFL_TEAM_COLORS[team]['tertiary'],
            'quaternary_color': NFL_TEAM_COLORS[team]['quaternary']
        })
    
    return pd.DataFrame(team_data)


def clean_team_abbreviations(data: pd.DataFrame, 
                           team_column: str = 'team') -> pd.DataFrame:
    """Clean and normalize team abbreviations in a DataFrame.
    
    Args:
        data: DataFrame containing team data
        team_column: Name of column containing team abbreviations
        
    Returns:
        DataFrame with normalized team abbreviations
        
    Raises:
        ValueError: If team_column doesn't exist or contains invalid teams
    """
    if team_column not in data.columns:
        raise ValueError(f"Column '{team_column}' not found in DataFrame")
    
    df = data.copy()
    
    # Normalize team abbreviations
    from .logos import normalize_team_abbreviation
    
    try:
        df[team_column] = df[team_column].apply(
            lambda x: normalize_team_abbreviation(str(x)) if pd.notna(x) else x
        )
    except ValueError as e:
        raise ValueError(f"Error normalizing teams in column '{team_column}': {e}")
    
    return df


def get_nflverse_info() -> Dict[str, Any]:
    """Get information about nflplotpy and nflverse ecosystem.
    
    Returns:
        Dictionary with package and ecosystem information
    """
    return {
        "package": "nflplotpy",
        "version": "0.1.0",
        "description": "Python NFL visualization package - equivalent to R's nflplotR",
        "ecosystem": "nflverse",
        "companion_packages": [
            "nfl_data_py",
            "nflreadr (R)",
            "nflplotR (R)",
            "nflfastR (R)"
        ],
        "data_sources": [
            "nflverse/nflverse-data",
            "Lee Sharpe's nfldata",
            "Wikipedia",
            "ESPN"
        ],
        "supported_backends": ["matplotlib", "plotly", "seaborn"]
    }


def nfl_sitrep() -> None:
    """Print comprehensive system and package information.
    
    Equivalent to R's nflverse_sitrep().
    """
    import sys
    import platform
    import matplotlib
    import pandas as pd
    import numpy as np
    from pathlib import Path
    
    print("=" * 60)
    print("🏈 nflplotpy System Report (Situation Report)")
    print("=" * 60)
    
    # Package information
    pkg_info = get_nflverse_info()
    print(f"\n📦 Package Information:")
    print(f"  nflplotpy version: {pkg_info['version']}")
    print(f"  Description: {pkg_info['description']}")
    
    # System information
    print(f"\n🖥️  System Information:")
    print(f"  Platform: {platform.platform()}")
    print(f"  Python version: {sys.version.split()[0]}")
    print(f"  Python executable: {sys.executable}")
    
    # Package versions
    print(f"\n📚 Dependencies:")
    print(f"  matplotlib: {matplotlib.__version__}")
    print(f"  pandas: {pd.__version__}")
    print(f"  numpy: {np.__version__}")
    
    # Optional dependencies
    optional_deps = []
    
    try:
        import plotly
        optional_deps.append(f"plotly: {plotly.__version__}")
    except ImportError:
        optional_deps.append("plotly: Not installed")
    
    try:
        import seaborn as sns
        optional_deps.append(f"seaborn: {sns.__version__}")
    except ImportError:
        optional_deps.append("seaborn: Not installed")
    
    try:
        import PIL
        optional_deps.append(f"PIL/Pillow: {PIL.__version__}")
    except ImportError:
        optional_deps.append("PIL/Pillow: Not installed")
    
    try:
        import requests
        optional_deps.append(f"requests: {requests.__version__}")
    except ImportError:
        optional_deps.append("requests: Not installed")
    
    for dep in optional_deps:
        print(f"  {dep}")
    
    # Asset cache information
    print(f"\n💾 Asset Cache:")
    try:
        from .assets import NFLAssetManager
        manager = NFLAssetManager()
        cache_info = manager.get_cache_info()
        
        print(f"  Cache directory: {cache_info['cache_dir']}")
        print(f"  Logos cached: {cache_info['logos_count']}")
        print(f"  Headshots cached: {cache_info['headshots_count']}")
        print(f"  Wordmarks cached: {cache_info['wordmarks_count']}")
        print(f"  Total cache size: {cache_info['total_size_bytes'] / 1024:.1f} KB")
        
    except Exception as e:
        print(f"  Error accessing cache: {e}")
    
    # Team data availability
    print(f"\n🏈 NFL Data:")
    available_teams = get_available_teams()
    print(f"  Teams available: {len(available_teams)}")
    print(f"  Sample teams: {', '.join(available_teams[:8])}...")
    
    # URL availability (sample check)
    print(f"\n🌐 URL Accessibility:")
    try:
        from .urls import get_url_manager
        manager = get_url_manager()
        
        # Test a few logo URLs
        import requests
        test_teams = ['KC', 'BUF', 'SF']
        working_urls = 0
        
        for team in test_teams:
            try:
                url = manager.get_logo_url(team)
                response = requests.head(url, timeout=5)
                if response.status_code < 400:
                    working_urls += 1
            except:
                pass
        
        print(f"  Logo URLs tested: {working_urls}/{len(test_teams)} working")
        
    except Exception as e:
        print(f"  URL check failed: {e}")
    
    # Plotting backends
    print(f"\n📊 Plotting Backends:")
    backends = pkg_info['supported_backends']
    for backend in backends:
        if backend == 'matplotlib':
            print(f"  ✅ {backend}: Available")
        elif backend == 'plotly':
            try:
                import plotly
                print(f"  ✅ {backend}: Available")
            except ImportError:
                print(f"  ❌ {backend}: Not installed")
        elif backend == 'seaborn':
            try:
                import seaborn
                print(f"  ✅ {backend}: Available") 
            except ImportError:
                print(f"  ❌ {backend}: Not installed")
    
    # Integration status
    print(f"\n🔗 Integration Status:")
    try:
        import nfl_data_py
        print(f"  ✅ nfl_data_py: Available (v{getattr(nfl_data_py, '__version__', 'unknown')})")
    except ImportError:
        print(f"  ❌ nfl_data_py: Not installed")
    
    # Jupyter notebook detection
    try:
        from IPython import get_ipython
        if get_ipython() is not None:
            print(f"  ✅ Jupyter/IPython: Available")
        else:
            print(f"  ❌ Jupyter/IPython: Not available")
    except ImportError:
        print(f"  ❌ Jupyter/IPython: Not installed")
    
    print(f"\n" + "=" * 60)
    print("Report complete! 🏈")
    
    # Recommendations
    recommendations = []
    
    try:
        import plotly
    except ImportError:
        recommendations.append("Install plotly for interactive plots: pip install plotly")
    
    try:
        import seaborn
    except ImportError:
        recommendations.append("Install seaborn for enhanced styling: pip install seaborn")
    
    try:
        import nfl_data_py
    except ImportError:
        recommendations.append("Install nfl_data_py for NFL data: pip install nfl_data_py")
    
    if recommendations:
        print(f"\n💡 Recommendations:")
        for rec in recommendations:
            print(f"  • {rec}")


def clear_all_cache() -> None:
    """Clear all nflplotpy caches.
    
    Clears logos, headshots, wordmarks, and any other cached assets.
    """
    try:
        from .assets import NFLAssetManager
        manager = NFLAssetManager()
        cache_info_before = manager.get_cache_info()
        
        print(f"Clearing cache at: {cache_info_before['cache_dir']}")
        print(f"Files before: {cache_info_before['logos_count'] + cache_info_before['headshots_count'] + cache_info_before['wordmarks_count']}")
        
        manager.clear_cache()
        
        cache_info_after = manager.get_cache_info()
        print(f"Files after: {cache_info_after['logos_count'] + cache_info_after['headshots_count'] + cache_info_after['wordmarks_count']}")
        print("✅ Cache cleared successfully!")
        
    except Exception as e:
        print(f"❌ Error clearing cache: {e}")


def validate_player_ids(player_ids: List[Union[str, int]]) -> Dict[str, bool]:
    """Validate player IDs for headshot availability.
    
    Args:
        player_ids: List of player IDs to validate
        
    Returns:
        Dictionary mapping player IDs to availability status
        
    Note:
        Currently checks format only. Full implementation would
        verify against ESPN/NFL databases.
    """
    results = {}
    
    for pid in player_ids:
        try:
            # Basic validation - ESPN IDs are typically numeric
            str_id = str(pid).strip()
            if str_id.isdigit() and len(str_id) >= 4:
                results[str_id] = True  # Assume valid for now
            else:
                results[str_id] = False
        except Exception:
            results[str(pid)] = False
    
    return results


def get_player_team_mapping(season: int = 2024) -> Dict[str, str]:
    """Get mapping of player names/IDs to current teams.
    
    Args:
        season: Season year for team assignments
        
    Returns:
        Dictionary mapping player identifiers to team abbreviations
        
    Note:
        This is a placeholder implementation. Full implementation would
        integrate with nfl_data_py roster data.
    """
    # Placeholder implementation with a few known players
    # Real implementation would query nfl_data_py roster data
    
    sample_mapping = {
        "Patrick Mahomes": "KC",
        "Josh Allen": "BUF", 
        "Lamar Jackson": "BAL",
        "Dak Prescott": "DAL",
        "Tua Tagovailoa": "MIA",
        "Aaron Rodgers": "NYJ",
        "Russell Wilson": "DEN",
        "Kyler Murray": "ARI"
    }
    
    warnings.warn(f"Player-team mapping not fully implemented for {season}. Using sample data.")
    return sample_mapping


def discover_team_from_colors(hex_color: str) -> Optional[str]:
    """Find NFL team that uses a given color.
    
    Args:
        hex_color: Hex color code (e.g., '#e31837')
        
    Returns:
        Team abbreviation if found, None otherwise
    """
    from .colors import NFL_TEAM_COLORS
    
    hex_color = hex_color.lower()
    
    for team, colors in NFL_TEAM_COLORS.items():
        for color_type, color_value in colors.items():
            if color_value.lower() == hex_color:
                return team
    
    return None


def get_season_info(season: int) -> Dict[str, Any]:
    """Get information about a specific NFL season.
    
    Args:
        season: Season year
        
    Returns:
        Dictionary with season information
        
    Note:
        This is a basic implementation. Could be enhanced with
        playoff info, schedule details, etc.
    """
    current_year = 2024  # Update as needed
    
    info = {
        "season": season,
        "is_current": season == current_year,
        "is_future": season > current_year,
        "is_historical": season < current_year,
        "data_available": season >= 1999,  # Based on nfl_data_py availability
        "regular_season_weeks": 18 if season >= 2021 else 17 if season >= 2021 else 16,
        "playoff_format": "expanded" if season >= 2020 else "traditional"
    }
    
    # Add notable season information
    notable_seasons = {
        2007: "Patriots 16-0 regular season",
        2013: "Peyton Manning 55 TD season", 
        2016: "Patriots 28-3 comeback",
        2020: "COVID-19 affected season",
        2021: "17-game regular season begins"
    }
    
    if season in notable_seasons:
        info["notable"] = notable_seasons[season]
    
    return info