"""NFL team logos and logo management."""

from typing import Dict, Optional
from PIL import Image
from .assets import NFLAssetManager


# NFL team logo URLs - Updated to match official nflverse data source
# Source: https://raw.githubusercontent.com/nflverse/nfldata/master/data/logos.csv
# All URLs updated 2025-01-08 to match nflplotR and nflverse standards
NFL_TEAM_LOGOS: Dict[str, str] = {
    'ARI': 'https://upload.wikimedia.org/wikipedia/en/thumb/7/72/Arizona_Cardinals_logo.svg/179px-Arizona_Cardinals_logo.svg.png',
    'ARZ': 'https://upload.wikimedia.org/wikipedia/en/thumb/7/72/Arizona_Cardinals_logo.svg/179px-Arizona_Cardinals_logo.svg.png',
    'ATL': 'https://upload.wikimedia.org/wikipedia/en/thumb/c/c5/Atlanta_Falcons_logo.svg/192px-Atlanta_Falcons_logo.svg.png',
    'BAL': 'https://upload.wikimedia.org/wikipedia/en/thumb/1/16/Baltimore_Ravens_logo.svg/193px-Baltimore_Ravens_logo.svg.png',
    'BLT': 'https://upload.wikimedia.org/wikipedia/en/thumb/1/16/Baltimore_Ravens_logo.svg/193px-Baltimore_Ravens_logo.svg.png',
    'BUF': 'https://upload.wikimedia.org/wikipedia/en/thumb/7/77/Buffalo_Bills_logo.svg/300px-Buffalo_Bills_logo.svg.png',
    'CAR': 'https://upload.wikimedia.org/wikipedia/en/thumb/1/1c/Carolina_Panthers_logo.svg/300px-Carolina_Panthers_logo.svg.png',
    'CHI': 'https://upload.wikimedia.org/wikipedia/commons/thumb/5/5c/Chicago_Bears_logo.svg/200px-Chicago_Bears_logo.svg.png',
    'CIN': 'https://upload.wikimedia.org/wikipedia/commons/thumb/8/81/Cincinnati_Bengals_logo.svg/300px-Cincinnati_Bengals_logo.svg.png',
    'CLE': 'https://upload.wikimedia.org/wikipedia/en/thumb/d/d9/Cleveland_Browns_logo.svg/300px-Cleveland_Browns_logo.svg.png',
    'CLV': 'https://upload.wikimedia.org/wikipedia/en/thumb/d/d9/Cleveland_Browns_logo.svg/300px-Cleveland_Browns_logo.svg.png',
    'DAL': 'https://upload.wikimedia.org/wikipedia/commons/thumb/1/15/Dallas_Cowboys.svg/192px-Dallas_Cowboys.svg.png',
    'DEN': 'https://upload.wikimedia.org/wikipedia/en/thumb/4/44/Denver_Broncos_logo.svg/300px-Denver_Broncos_logo.svg.png',
    'DET': 'https://upload.wikimedia.org/wikipedia/en/thumb/7/71/Detroit_Lions_logo.svg/300px-Detroit_Lions_logo.svg.png',
    'GB': 'https://upload.wikimedia.org/wikipedia/commons/thumb/5/50/Green_Bay_Packers_logo.svg/300px-Green_Bay_Packers_logo.svg.png',
    'GNB': 'https://upload.wikimedia.org/wikipedia/commons/thumb/5/50/Green_Bay_Packers_logo.svg/300px-Green_Bay_Packers_logo.svg.png',
    'HOU': 'https://upload.wikimedia.org/wikipedia/en/thumb/2/28/Houston_Texans_logo.svg/300px-Houston_Texans_logo.svg.png',
    'HST': 'https://upload.wikimedia.org/wikipedia/en/thumb/2/28/Houston_Texans_logo.svg/300px-Houston_Texans_logo.svg.png',
    'IND': 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/00/Indianapolis_Colts_logo.svg/300px-Indianapolis_Colts_logo.svg.png',
    'JAC': 'https://upload.wikimedia.org/wikipedia/en/thumb/7/74/Jacksonville_Jaguars_logo.svg/200px-Jacksonville_Jaguars_logo.svg.png',
    'JAX': 'https://upload.wikimedia.org/wikipedia/en/thumb/7/74/Jacksonville_Jaguars_logo.svg/200px-Jacksonville_Jaguars_logo.svg.png',
    'KC': 'https://upload.wikimedia.org/wikipedia/en/thumb/e/e1/Kansas_City_Chiefs_logo.svg/300px-Kansas_City_Chiefs_logo.svg.png',
    'KAN': 'https://upload.wikimedia.org/wikipedia/en/thumb/e/e1/Kansas_City_Chiefs_logo.svg/300px-Kansas_City_Chiefs_logo.svg.png',
    'LA': 'https://upload.wikimedia.org/wikipedia/en/thumb/8/8a/Los_Angeles_Rams_logo.svg/300px-Los_Angeles_Rams_logo.svg.png',
    'LAC': 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a6/Los_Angeles_Chargers_logo.svg/200px-Los_Angeles_Chargers_logo.svg.png',
    'LAR': 'https://upload.wikimedia.org/wikipedia/en/thumb/8/8a/Los_Angeles_Rams_logo.svg/300px-Los_Angeles_Rams_logo.svg.png',
    'LV': 'https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Las_Vegas_Raiders_logo.svg/200px-Las_Vegas_Raiders_logo.svg.png',
    'LVR': 'https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Las_Vegas_Raiders_logo.svg/200px-Las_Vegas_Raiders_logo.svg.png',
    'MIA': 'https://upload.wikimedia.org/wikipedia/en/thumb/3/37/Miami_Dolphins_logo.svg/300px-Miami_Dolphins_logo.svg.png',
    'MIN': 'https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Minnesota_Vikings_logo.svg/300px-Minnesota_Vikings_logo.svg.png',
    'NE': 'https://upload.wikimedia.org/wikipedia/en/thumb/b/b9/New_England_Patriots_logo.svg/300px-New_England_Patriots_logo.svg.png',
    'NO': 'https://upload.wikimedia.org/wikipedia/commons/thumb/5/50/New_Orleans_Saints_logo.svg/200px-New_Orleans_Saints_logo.svg.png',
    'NOR': 'https://upload.wikimedia.org/wikipedia/commons/thumb/5/50/New_Orleans_Saints_logo.svg/200px-New_Orleans_Saints_logo.svg.png',
    'NYG': 'https://upload.wikimedia.org/wikipedia/commons/thumb/6/60/New_York_Giants_logo.svg/200px-New_York_Giants_logo.svg.png',
    'NYJ': 'https://upload.wikimedia.org/wikipedia/en/thumb/6/6b/New_York_Jets_logo.svg/300px-New_York_Jets_logo.svg.png',
    'OAK': 'https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Las_Vegas_Raiders_logo.svg/200px-Las_Vegas_Raiders_logo.svg.png',
    'PHI': 'https://upload.wikimedia.org/wikipedia/en/thumb/8/8e/Philadelphia_Eagles_logo.svg/300px-Philadelphia_Eagles_logo.svg.png',
    'PIT': 'https://upload.wikimedia.org/wikipedia/commons/thumb/d/de/Pittsburgh_Steelers_logo.svg/300px-Pittsburgh_Steelers_logo.svg.png',
    'SD': 'https://upload.wikimedia.org/wikipedia/en/thumb/7/72/Los_Angeles_Chargers_logo.svg/300px-Los_Angeles_Chargers_logo.svg.png',
    'SEA': 'https://upload.wikimedia.org/wikipedia/en/thumb/8/8e/Seattle_Seahawks_logo.svg/300px-Seattle_Seahawks_logo.svg.png',
    'SF': 'https://upload.wikimedia.org/wikipedia/commons/thumb/3/3a/San_Francisco_49ers_logo.svg/300px-San_Francisco_49ers_logo.svg.png',
    'SFO': 'https://upload.wikimedia.org/wikipedia/commons/thumb/3/3a/San_Francisco_49ers_logo.svg/300px-San_Francisco_49ers_logo.svg.png',
    'STL': 'https://upload.wikimedia.org/wikipedia/en/thumb/8/8a/Los_Angeles_Rams_logo.svg/300px-Los_Angeles_Rams_logo.svg.png',
    'TB': 'https://upload.wikimedia.org/wikipedia/en/thumb/a/a2/Tampa_Bay_Buccaneers_logo.svg/300px-Tampa_Bay_Buccaneers_logo.svg.png',
    'TEN': 'https://upload.wikimedia.org/wikipedia/en/thumb/c/c1/Tennessee_Titans_logo.svg/300px-Tennessee_Titans_logo.svg.png',
    'WAS': 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Washington_Commanders_logo.svg/200px-Washington_Commanders_logo.svg.png',
    'WSH': 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Washington_Commanders_logo.svg/200px-Washington_Commanders_logo.svg.png',
    # Conference logos
    'AFC': 'https://upload.wikimedia.org/wikipedia/en/thumb/7/7b/American_Football_Conference_logo.svg/200px-American_Football_Conference_logo.svg.png',
    'NFC': 'https://upload.wikimedia.org/wikipedia/en/thumb/5/5e/National_Football_Conference_logo.svg/200px-National_Football_Conference_logo.svg.png',
    'NFL': 'https://upload.wikimedia.org/wikipedia/en/thumb/a/a2/National_Football_League_logo.svg/200px-National_Football_League_logo.svg.png'
}


# Team abbreviation mappings for consistency
TEAM_ABBREVIATION_MAP: Dict[str, str] = {
    'ARZ': 'ARI',
    'BLT': 'BAL', 
    'CLV': 'CLE',
    'GNB': 'GB',
    'HST': 'HOU',
    'JAX': 'JAC',
    'KAN': 'KC',
    'LA': 'LAR',
    'LVR': 'LV',
    'NE': 'NE',
    'NO': 'NO',
    'NOR': 'NO',
    'OAK': 'LV',
    'SD': 'LAC',
    'SDG': 'LAC',
    'SF': 'SF',
    'SFO': 'SF',
    'STL': 'LAR',
    'TB': 'TB',
    'WSH': 'WAS'
}


def normalize_team_abbreviation(team: str) -> str:
    """Normalize team abbreviation to standard format.
    
    Args:
        team: Team abbreviation
        
    Returns:
        Normalized team abbreviation
        
    Raises:
        ValueError: If team abbreviation is not valid
    """
    team = team.upper()
    
    # Return direct match first
    if team in NFL_TEAM_LOGOS:
        return team
        
    # Check if it needs mapping
    if team in TEAM_ABBREVIATION_MAP:
        return TEAM_ABBREVIATION_MAP[team]
        
    raise ValueError(f"Invalid team abbreviation: {team}")


def get_team_logo_url(team: str) -> str:
    """Get logo URL for a team.
    
    Args:
        team: Team abbreviation
        
    Returns:
        URL to team logo
        
    Raises:
        ValueError: If team abbreviation is not valid
    """
    normalized_team = normalize_team_abbreviation(team)
    return NFL_TEAM_LOGOS[normalized_team]


def get_available_teams() -> list[str]:
    """Get list of all available team abbreviations.
    
    Returns:
        List of team abbreviations
    """
    return sorted(list(set(NFL_TEAM_LOGOS.keys()) - {'AFC', 'NFC', 'NFL'}))


def get_conference_teams(conference: str) -> list[str]:
    """Get teams in a specific conference.
    
    Args:
        conference: 'AFC' or 'NFC'
        
    Returns:
        List of team abbreviations in the conference
        
    Raises:
        ValueError: If conference is not valid
    """
    if conference.upper() not in ['AFC', 'NFC']:
        raise ValueError("Conference must be 'AFC' or 'NFC'")
    
    # This is a simplified mapping - in a real implementation,
    # you'd want to load this from nfl_data_py or a data file
    afc_teams = ['BAL', 'BUF', 'CIN', 'CLE', 'DEN', 'HOU', 'IND', 'JAC',
                 'KC', 'LV', 'LAC', 'MIA', 'NE', 'NYJ', 'PIT', 'TEN']
    nfc_teams = ['ARI', 'ATL', 'CAR', 'CHI', 'DAL', 'DET', 'GB', 'LAR',
                 'MIN', 'NO', 'NYG', 'PHI', 'SEA', 'SF', 'TB', 'WAS']
    
    if conference.upper() == 'AFC':
        return afc_teams
    else:
        return nfc_teams


# Singleton asset manager for easy access
_asset_manager: Optional[NFLAssetManager] = None

def get_asset_manager() -> NFLAssetManager:
    """Get the singleton asset manager instance."""
    global _asset_manager
    if _asset_manager is None:
        _asset_manager = NFLAssetManager()
    return _asset_manager


def get_team_logo(team: str, format: str = "png", force_refresh: bool = False) -> Image.Image:
    """Get NFL team logo as PIL Image.
    
    This is a convenience function that uses the singleton asset manager.
    
    Args:
        team: Team abbreviation (e.g., 'ARI', 'ATL')
        format: Image format ('png', 'svg')
        force_refresh: If True, re-download even if cached
        
    Returns:
        PIL Image object
    """
    manager = get_asset_manager()
    return manager.get_logo(team, format, force_refresh)