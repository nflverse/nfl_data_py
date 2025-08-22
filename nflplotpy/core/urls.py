"""Enhanced URL management for NFL assets.

Provides comprehensive URL mapping and management for team logos,
player headshots, wordmarks, and other NFL assets.
"""

import re
from typing import Dict, Optional, List, Union
import warnings

from .logos import NFL_TEAM_LOGOS


# Enhanced team wordmark URLs
# These are placeholders - in production, these would be actual wordmark URLs
NFL_TEAM_WORDMARKS: Dict[str, str] = {
    'ARI': 'https://upload.wikimedia.org/wikipedia/en/thumb/7/72/Arizona_Cardinals_logo.svg/300px-Arizona_Cardinals_logo.svg.png',
    'ATL': 'https://upload.wikimedia.org/wikipedia/en/thumb/c/c5/Atlanta_Falcons_logo.svg/300px-Atlanta_Falcons_logo.svg.png',
    'BAL': 'https://upload.wikimedia.org/wikipedia/en/thumb/1/16/Baltimore_Ravens_logo.svg/300px-Baltimore_Ravens_logo.svg.png',
    'BUF': 'https://upload.wikimedia.org/wikipedia/en/thumb/7/77/Buffalo_Bills_logo.svg/300px-Buffalo_Bills_logo.svg.png',
    'CAR': 'https://upload.wikimedia.org/wikipedia/en/thumb/1/1c/Carolina_Panthers_logo.svg/300px-Carolina_Panthers_logo.svg.png',
    'CHI': 'https://upload.wikimedia.org/wikipedia/commons/thumb/5/5c/Chicago_Bears_logo.svg/300px-Chicago_Bears_logo.svg.png',
    'CIN': 'https://upload.wikimedia.org/wikipedia/commons/thumb/8/81/Cincinnati_Bengals_logo.svg/300px-Cincinnati_Bengals_logo.svg.png',
    'CLE': 'https://upload.wikimedia.org/wikipedia/en/thumb/d/d9/Cleveland_Browns_logo.svg/300px-Cleveland_Browns_logo.svg.png',
    'DAL': 'https://upload.wikimedia.org/wikipedia/commons/thumb/1/15/Dallas_Cowboys.svg/300px-Dallas_Cowboys.svg.png',
    'DEN': 'https://upload.wikimedia.org/wikipedia/en/thumb/4/44/Denver_Broncos_logo.svg/300px-Denver_Broncos_logo.svg.png',
    'DET': 'https://upload.wikimedia.org/wikipedia/en/thumb/7/71/Detroit_Lions_logo.svg/300px-Detroit_Lions_logo.svg.png',
    'GB': 'https://upload.wikimedia.org/wikipedia/commons/thumb/5/50/Green_Bay_Packers_logo.svg/300px-Green_Bay_Packers_logo.svg.png',
    'HOU': 'https://upload.wikimedia.org/wikipedia/en/thumb/2/28/Houston_Texans_logo.svg/300px-Houston_Texans_logo.svg.png',
    'IND': 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/00/Indianapolis_Colts_logo.svg/300px-Indianapolis_Colts_logo.svg.png',
    'JAC': 'https://upload.wikimedia.org/wikipedia/en/thumb/7/74/Jacksonville_Jaguars_logo.svg/300px-Jacksonville_Jaguars_logo.svg.png',
    'KC': 'https://upload.wikimedia.org/wikipedia/en/thumb/e/e1/Kansas_City_Chiefs_logo.svg/300px-Kansas_City_Chiefs_logo.svg.png',
    'LV': 'https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Las_Vegas_Raiders_logo.svg/300px-Las_Vegas_Raiders_logo.svg.png',
    'LAC': 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/a6/Los_Angeles_Chargers_logo.svg/300px-Los_Angeles_Chargers_logo.svg.png',
    'LAR': 'https://upload.wikimedia.org/wikipedia/en/thumb/8/8a/Los_Angeles_Rams_logo.svg/300px-Los_Angeles_Rams_logo.svg.png',
    'MIA': 'https://upload.wikimedia.org/wikipedia/en/thumb/3/37/Miami_Dolphins_logo.svg/300px-Miami_Dolphins_logo.svg.png',
    'MIN': 'https://upload.wikimedia.org/wikipedia/en/thumb/4/48/Minnesota_Vikings_logo.svg/300px-Minnesota_Vikings_logo.svg.png',
    'NE': 'https://upload.wikimedia.org/wikipedia/en/thumb/b/b9/New_England_Patriots_logo.svg/300px-New_England_Patriots_logo.svg.png',
    'NO': 'https://upload.wikimedia.org/wikipedia/commons/thumb/5/50/New_Orleans_Saints_logo.svg/300px-New_Orleans_Saints_logo.svg.png',
    'NYG': 'https://upload.wikimedia.org/wikipedia/commons/thumb/6/60/New_York_Giants_logo.svg/300px-New_York_Giants_logo.svg.png',
    'NYJ': 'https://upload.wikimedia.org/wikipedia/en/thumb/6/6b/New_York_Jets_logo.svg/300px-New_York_Jets_logo.svg.png',
    'PHI': 'https://upload.wikimedia.org/wikipedia/en/thumb/8/8e/Philadelphia_Eagles_logo.svg/300px-Philadelphia_Eagles_logo.svg.png',
    'PIT': 'https://upload.wikimedia.org/wikipedia/commons/thumb/d/de/Pittsburgh_Steelers_logo.svg/300px-Pittsburgh_Steelers_logo.svg.png',
    'SEA': 'https://upload.wikimedia.org/wikipedia/en/thumb/8/8e/Seattle_Seahawks_logo.svg/300px-Seattle_Seahawks_logo.svg.png',
    'SF': 'https://upload.wikimedia.org/wikipedia/commons/thumb/3/3a/San_Francisco_49ers_logo.svg/300px-San_Francisco_49ers_logo.svg.png',
    'TB': 'https://upload.wikimedia.org/wikipedia/en/thumb/a/a2/Tampa_Bay_Buccaneers_logo.svg/300px-Tampa_Bay_Buccaneers_logo.svg.png',
    'TEN': 'https://upload.wikimedia.org/wikipedia/en/thumb/c/c1/Tennessee_Titans_logo.svg/300px-Tennessee_Titans_logo.svg.png',
    'WAS': 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0c/Washington_Commanders_logo.svg/300px-Washington_Commanders_logo.svg.png',
}


class PlayerHeadshotURLBuilder:
    """Builds URLs for NFL player headshots from various sources."""
    
    # ESPN headshot URL patterns
    ESPN_HEADSHOT_BASE = "https://a.espncdn.com/i/headshots/nfl/players/full/{player_id}.png"
    ESPN_HEADSHOT_SMALL = "https://a.espncdn.com/i/headshots/nfl/players/small/{player_id}.png"
    
    # NFL.com headshot patterns (these would need to be verified)
    NFL_HEADSHOT_BASE = "https://static.www.nfl.com/league/api/clubs/logos/{team}/{player_id}.png"
    
    @classmethod
    def get_espn_headshot_url(cls, player_id: Union[str, int], size: str = "full") -> str:
        """Get ESPN headshot URL for a player.
        
        Args:
            player_id: ESPN player ID
            size: Size variant ('full', 'small')
            
        Returns:
            URL to player headshot
        """
        if size == "full":
            return cls.ESPN_HEADSHOT_BASE.format(player_id=player_id)
        elif size == "small":
            return cls.ESPN_HEADSHOT_SMALL.format(player_id=player_id)
        else:
            raise ValueError(f"Invalid size: {size}. Use 'full' or 'small'")
    
    @classmethod
    def get_nfl_headshot_url(cls, player_id: str, team: str) -> str:
        """Get NFL.com headshot URL for a player.
        
        Args:
            player_id: NFL player ID
            team: Team abbreviation
            
        Returns:
            URL to player headshot
            
        Note:
            This is a placeholder implementation. Actual NFL.com URLs
            would need to be researched and verified.
        """
        return cls.NFL_HEADSHOT_BASE.format(team=team.lower(), player_id=player_id)
    
    @classmethod
    def build_headshot_urls(cls, player_id: Union[str, int], 
                          sources: List[str] = None) -> Dict[str, str]:
        """Build headshot URLs from multiple sources.
        
        Args:
            player_id: Player identifier
            sources: List of sources to try ('espn', 'nfl')
            
        Returns:
            Dictionary mapping source names to URLs
        """
        if sources is None:
            sources = ['espn']
            
        urls = {}
        
        for source in sources:
            try:
                if source == 'espn':
                    urls['espn_full'] = cls.get_espn_headshot_url(player_id, 'full')
                    urls['espn_small'] = cls.get_espn_headshot_url(player_id, 'small')
                elif source == 'nfl':
                    # Would need team info for NFL.com URLs
                    pass
            except Exception as e:
                warnings.warn(f"Could not build {source} URL for {player_id}: {e}")
        
        return urls


class AssetURLManager:
    """Comprehensive manager for all NFL asset URLs."""
    
    def __init__(self):
        """Initialize URL manager."""
        self.logos = NFL_TEAM_LOGOS.copy()
        self.wordmarks = NFL_TEAM_WORDMARKS.copy()
        self.headshot_builder = PlayerHeadshotURLBuilder()
    
    def get_logo_url(self, team: str) -> str:
        """Get team logo URL.
        
        Args:
            team: Team abbreviation
            
        Returns:
            URL to team logo
        """
        team = team.upper()
        if team not in self.logos:
            raise ValueError(f"No logo URL found for team: {team}")
        return self.logos[team]
    
    def get_wordmark_url(self, team: str) -> str:
        """Get team wordmark URL.
        
        Args:
            team: Team abbreviation
            
        Returns:
            URL to team wordmark
        """
        team = team.upper()
        if team not in self.wordmarks:
            warnings.warn(f"No wordmark URL found for team: {team}, using logo")
            return self.get_logo_url(team)
        return self.wordmarks[team]
    
    def get_headshot_urls(self, player_id: Union[str, int], 
                         sources: List[str] = None) -> Dict[str, str]:
        """Get player headshot URLs.
        
        Args:
            player_id: Player identifier
            sources: Sources to try
            
        Returns:
            Dictionary of source -> URL mappings
        """
        return self.headshot_builder.build_headshot_urls(player_id, sources)
    
    def add_custom_logo_url(self, team: str, url: str):
        """Add or override team logo URL.
        
        Args:
            team: Team abbreviation
            url: URL to logo image
        """
        self.logos[team.upper()] = url
    
    def add_custom_wordmark_url(self, team: str, url: str):
        """Add or override team wordmark URL.
        
        Args:
            team: Team abbreviation
            url: URL to wordmark image
        """
        self.wordmarks[team.upper()] = url
    
    def validate_url(self, url: str) -> bool:
        """Validate that a URL is properly formatted.
        
        Args:
            url: URL to validate
            
        Returns:
            True if URL appears valid
        """
        # Basic URL validation
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        return url_pattern.match(url) is not None
    
    def get_all_urls(self) -> Dict[str, Dict[str, str]]:
        """Get all managed URLs.
        
        Returns:
            Dictionary with 'logos' and 'wordmarks' sections
        """
        return {
            'logos': self.logos.copy(),
            'wordmarks': self.wordmarks.copy()
        }
    
    def update_urls_from_dict(self, url_dict: Dict[str, Dict[str, str]]):
        """Update URLs from dictionary.
        
        Args:
            url_dict: Dictionary with 'logos' and/or 'wordmarks' keys
        """
        if 'logos' in url_dict:
            self.logos.update(url_dict['logos'])
        
        if 'wordmarks' in url_dict:
            self.wordmarks.update(url_dict['wordmarks'])


# Singleton URL manager
_url_manager: Optional[AssetURLManager] = None

def get_url_manager() -> AssetURLManager:
    """Get singleton URL manager instance."""
    global _url_manager
    if _url_manager is None:
        _url_manager = AssetURLManager()
    return _url_manager


def get_team_wordmark_url(team: str) -> str:
    """Get team wordmark URL.
    
    Convenience function using singleton URL manager.
    
    Args:
        team: Team abbreviation
        
    Returns:
        URL to team wordmark
    """
    manager = get_url_manager()
    return manager.get_wordmark_url(team)


def get_player_headshot_urls(player_id: Union[str, int],
                           sources: List[str] = None) -> Dict[str, str]:
    """Get player headshot URLs from multiple sources.
    
    Convenience function using singleton URL manager.
    
    Args:
        player_id: Player identifier
        sources: Sources to try ('espn', 'nfl')
        
    Returns:
        Dictionary mapping sources to URLs
        
    Example:
        >>> urls = get_player_headshot_urls("4035538")  # Patrick Mahomes
        >>> print(urls['espn_full'])
    """
    manager = get_url_manager()
    return manager.get_headshot_urls(player_id, sources)


def discover_player_id(player_name: str, team: Optional[str] = None) -> Optional[str]:
    """Attempt to discover ESPN player ID from name.
    
    Args:
        player_name: Player's full name (e.g., "Patrick Mahomes")
        team: Team abbreviation (helps with disambiguation)
        
    Returns:
        ESPN player ID if found, None otherwise
        
    Note:
        This is a placeholder implementation. A full implementation would
        require integration with ESPN's API or a player ID database.
    """
    # Placeholder implementation - in production, this would query an API
    # or maintained database of player name -> ID mappings
    
    # Common test cases for development
    test_players = {
        "patrick mahomes": "3139477",
        "josh allen": "3918298", 
        "tom brady": "2330",
        "aaron rodgers": "8439"
    }
    
    normalized_name = player_name.lower().strip()
    if normalized_name in test_players:
        return test_players[normalized_name]
    
    warnings.warn(f"Player ID lookup not implemented for: {player_name}")
    return None


def validate_all_urls() -> Dict[str, List[str]]:
    """Validate all managed URLs for accessibility.
    
    Returns:
        Dictionary with 'valid' and 'invalid' URL lists
        
    Note:
        This function makes HTTP requests to check URL accessibility.
        Use sparingly to avoid overwhelming servers.
    """
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    
    manager = get_url_manager()
    all_urls = {**manager.logos, **manager.wordmarks}
    
    valid_urls = []
    invalid_urls = []
    
    def check_url(url):
        try:
            response = requests.head(url, timeout=10)
            return url, response.status_code < 400
        except Exception:
            return url, False
    
    # Check URLs with rate limiting
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(check_url, url): url for url in all_urls.values()}
        
        for future in as_completed(futures):
            url, is_valid = future.result()
            
            if is_valid:
                valid_urls.append(url)
            else:
                invalid_urls.append(url)
            
            # Rate limiting
            time.sleep(0.1)
    
    return {
        'valid': valid_urls,
        'invalid': invalid_urls
    }