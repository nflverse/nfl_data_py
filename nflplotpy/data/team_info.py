"""Team data and metadata management."""

import pandas as pd
from typing import Dict, Any, Optional, List

from ..core.utils import get_team_info
from ..core.colors import NFL_TEAM_COLORS
from ..core.logos import NFL_TEAM_LOGOS


def load_team_data(teams: Optional[List[str]] = None) -> pd.DataFrame:
    """Load comprehensive team data.
    
    Args:
        teams: Specific teams to load. If None, loads all teams.
        
    Returns:
        DataFrame with team information
    """
    return get_team_info(teams)


def get_team_mapping() -> Dict[str, Dict[str, Any]]:
    """Get complete team mapping with all available data.
    
    Returns:
        Dictionary mapping team abbreviations to team data
    """
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
    
    team_nicknames = {
        'ARI': 'Cardinals', 'ATL': 'Falcons', 'BAL': 'Ravens',
        'BUF': 'Bills', 'CAR': 'Panthers', 'CHI': 'Bears',
        'CIN': 'Bengals', 'CLE': 'Browns', 'DAL': 'Cowboys',
        'DEN': 'Broncos', 'DET': 'Lions', 'GB': 'Packers',
        'HOU': 'Texans', 'IND': 'Colts', 'JAC': 'Jaguars',
        'KC': 'Chiefs', 'LV': 'Raiders', 'LAC': 'Chargers',
        'LAR': 'Rams', 'MIA': 'Dolphins', 'MIN': 'Vikings',
        'NE': 'Patriots', 'NO': 'Saints', 'NYG': 'Giants',
        'NYJ': 'Jets', 'PHI': 'Eagles', 'PIT': 'Steelers',
        'SEA': 'Seahawks', 'SF': '49ers', 'TB': 'Buccaneers',
        'TEN': 'Titans', 'WAS': 'Commanders'
    }
    
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
    
    team_mapping = {}
    
    for team in team_names.keys():
        division = divisions.get(team, 'Unknown')
        conference = division.split()[0] if division != 'Unknown' else 'Unknown'
        
        team_mapping[team] = {
            'abbreviation': team,
            'name': team_names[team],
            'nickname': team_nicknames[team],
            'conference': conference,
            'division': division,
            'colors': NFL_TEAM_COLORS.get(team, {}),
            'logo_url': NFL_TEAM_LOGOS.get(team, '')
        }
    
    return team_mapping