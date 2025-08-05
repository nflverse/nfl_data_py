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