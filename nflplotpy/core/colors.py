"""NFL team colors and color palette management."""

from typing import Dict, List, Optional, Union, Tuple
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np


# NFL team colors based on nflverse data
# Source: https://raw.githubusercontent.com/leesharpe/nfldata/master/data/teamcolors.csv
NFL_TEAM_COLORS: Dict[str, Dict[str, str]] = {
    'ARI': {'primary': '#97233f', 'secondary': '#000000', 'tertiary': '#ffb612', 'quaternary': '#a5acaf'},
    'ARZ': {'primary': '#97233f', 'secondary': '#000000', 'tertiary': '#ffb612', 'quaternary': '#a5acaf'},
    'ATL': {'primary': '#a71930', 'secondary': '#000000', 'tertiary': '#a5acaf', 'quaternary': '#a30d2d'},
    'BAL': {'primary': '#241773', 'secondary': '#000000', 'tertiary': '#9E7C0C', 'quaternary': '#c60c30'},
    'BLT': {'primary': '#241773', 'secondary': '#000000', 'tertiary': '#9E7C0C', 'quaternary': '#c60c30'},
    'BUF': {'primary': '#00338d', 'secondary': '#c60c30', 'tertiary': '#ffffff', 'quaternary': '#c60c30'},
    'CAR': {'primary': '#0085ca', 'secondary': '#101820', 'tertiary': '#bfc0bf', 'quaternary': '#bfc0bf'},
    'CHI': {'primary': '#0b162a', 'secondary': '#c83803', 'tertiary': '#ffffff', 'quaternary': '#c83803'},
    'CIN': {'primary': '#fb4f14', 'secondary': '#000000', 'tertiary': '#ffffff', 'quaternary': '#000000'},
    'CLE': {'primary': '#311d00', 'secondary': '#ff3c00', 'tertiary': '#ffffff', 'quaternary': '#ff3c00'},
    'CLV': {'primary': '#311d00', 'secondary': '#ff3c00', 'tertiary': '#ffffff', 'quaternary': '#ff3c00'},
    'DAL': {'primary': '#041e42', 'secondary': '#869397', 'tertiary': '#ffffff', 'quaternary': '#00338d'},
    'DEN': {'primary': '#fb4f14', 'secondary': '#002244', 'tertiary': '#ffffff', 'quaternary': '#002244'},
    'DET': {'primary': '#0076b6', 'secondary': '#b0b7bc', 'tertiary': '#000000', 'quaternary': '#ffffff'},
    'GB': {'primary': '#203731', 'secondary': '#ffb612', 'tertiary': '#ffffff', 'quaternary': '#ffb612'},
    'GNB': {'primary': '#203731', 'secondary': '#ffb612', 'tertiary': '#ffffff', 'quaternary': '#ffb612'},
    'HOU': {'primary': '#03202f', 'secondary': '#a71930', 'tertiary': '#ffffff', 'quaternary': '#a71930'},
    'HST': {'primary': '#03202f', 'secondary': '#a71930', 'tertiary': '#ffffff', 'quaternary': '#a71930'},
    'IND': {'primary': '#002c5f', 'secondary': '#a2aaad', 'tertiary': '#ffffff', 'quaternary': '#a2aaad'},
    'JAC': {'primary': '#006778', 'secondary': '#9f792c', 'tertiary': '#000000', 'quaternary': '#d7a22a'},
    'JAX': {'primary': '#006778', 'secondary': '#9f792c', 'tertiary': '#000000', 'quaternary': '#d7a22a'},
    'KC': {'primary': '#e31837', 'secondary': '#ffb612', 'tertiary': '#ffffff', 'quaternary': '#ffb612'},
    'KAN': {'primary': '#e31837', 'secondary': '#ffb612', 'tertiary': '#ffffff', 'quaternary': '#ffb612'},
    'LA': {'primary': '#003594', 'secondary': '#ffa300', 'tertiary': '#ffffff', 'quaternary': '#ff8200'},
    'LAC': {'primary': '#0080c6', 'secondary': '#ffc20e', 'tertiary': '#ffffff', 'quaternary': '#002a5e'},
    'LAR': {'primary': '#003594', 'secondary': '#ffa300', 'tertiary': '#ffffff', 'quaternary': '#ff8200'},
    'LV': {'primary': '#000000', 'secondary': '#a5acaf', 'tertiary': '#ffffff', 'quaternary': '#a5acaf'},
    'LVR': {'primary': '#000000', 'secondary': '#a5acaf', 'tertiary': '#ffffff', 'quaternary': '#a5acaf'},
    'MIA': {'primary': '#008e97', 'secondary': '#fc4c02', 'tertiary': '#005778', 'quaternary': '#fc4c02'},
    'MIN': {'primary': '#4f2683', 'secondary': '#ffc62f', 'tertiary': '#ffffff', 'quaternary': '#ffc62f'},
    'NE': {'primary': '#002244', 'secondary': '#c60c30', 'tertiary': '#b0b7bc', 'quaternary': '#002244'},
    'NO': {'primary': '#d3bc8d', 'secondary': '#101820', 'tertiary': '#ffffff', 'quaternary': '#101820'},
    'NOS': {'primary': '#d3bc8d', 'secondary': '#101820', 'tertiary': '#ffffff', 'quaternary': '#101820'},
    'NYG': {'primary': '#0b2265', 'secondary': '#a71930', 'tertiary': '#a5acaf', 'quaternary': '#a71930'},
    'NYJ': {'primary': '#125740', 'secondary': '#ffffff', 'tertiary': '#000000', 'quaternary': '#ffffff'},
    'OAK': {'primary': '#000000', 'secondary': '#a5acaf', 'tertiary': '#ffffff', 'quaternary': '#a5acaf'},
    'PHI': {'primary': '#004c54', 'secondary': '#a5acaf', 'tertiary': '#acc0c6', 'quaternary': '#000000'},
    'PIT': {'primary': '#ffb612', 'secondary': '#101820', 'tertiary': '#ffffff', 'quaternary': '#c60c30'},
    'SD': {'primary': '#0080c6', 'secondary': '#ffc20e', 'tertiary': '#ffffff', 'quaternary': '#002a5e'},
    'SDG': {'primary': '#0080c6', 'secondary': '#ffc20e', 'tertiary': '#ffffff', 'quaternary': '#002a5e'},
    'SEA': {'primary': '#002244', 'secondary': '#69be28', 'tertiary': '#a5acaf', 'quaternary': '#69be28'},
    'SF': {'primary': '#aa0000', 'secondary': '#b3995d', 'tertiary': '#ffffff', 'quaternary': '#b3995d'},
    'SFO': {'primary': '#aa0000', 'secondary': '#b3995d', 'tertiary': '#ffffff', 'quaternary': '#b3995d'},
    'STL': {'primary': '#003594', 'secondary': '#ffa300', 'tertiary': '#ffffff', 'quaternary': '#ff8200'},
    'TB': {'primary': '#d50a0a', 'secondary': '#ff7900', 'tertiary': '#0a0a08', 'quaternary': '#b1babf'},
    'TEN': {'primary': '#0c2340', 'secondary': '#4b92db', 'tertiary': '#c8102e', 'quaternary': '#a5acaf'},
    'WAS': {'primary': '#773141', 'secondary': '#ffb612', 'tertiary': '#000000', 'quaternary': '#5b2b2f'},
    'WSH': {'primary': '#773141', 'secondary': '#ffb612', 'tertiary': '#000000', 'quaternary': '#5b2b2f'},
}


class NFLColorPalette:
    """Manages NFL team colors and creates color palettes for visualization."""
    
    def __init__(self):
        """Initialize the color palette manager."""
        self.team_colors = NFL_TEAM_COLORS
    
    def get_team_colors(self, teams: Union[str, List[str]], 
                       color_type: str = "primary") -> Union[str, List[str]]:
        """Get colors for specified teams.
        
        Args:
            teams: Team abbreviation(s)
            color_type: Type of color ('primary', 'secondary', 'tertiary', 'quaternary')
            
        Returns:
            Color hex code(s)
            
        Raises:
            ValueError: If team or color_type is invalid
        """
        if color_type not in ['primary', 'secondary', 'tertiary', 'quaternary']:
            raise ValueError(f"Invalid color_type: {color_type}")
        
        if isinstance(teams, str):
            teams = [teams]
        
        colors = []
        for team in teams:
            team = team.upper()
            if team not in self.team_colors:
                raise ValueError(f"Invalid team abbreviation: {team}")
            colors.append(self.team_colors[team][color_type])
        
        return colors[0] if len(colors) == 1 else colors
    
    def get_team_color_dict(self, team: str) -> Dict[str, str]:
        """Get all colors for a team.
        
        Args:
            team: Team abbreviation
            
        Returns:
            Dictionary with all color types for the team
            
        Raises:
            ValueError: If team is invalid
        """
        team = team.upper()
        if team not in self.team_colors:
            raise ValueError(f"Invalid team abbreviation: {team}")
        
        return self.team_colors[team].copy()
    
    def create_team_palette(self, teams: List[str], 
                           color_type: str = "primary") -> List[str]:
        """Create color palette for multiple teams.
        
        Args:
            teams: List of team abbreviations
            color_type: Type of color to use
            
        Returns:
            List of hex color codes
        """
        return self.get_team_colors(teams, color_type)
    
    def create_gradient(self, team1: str, team2: str, 
                       n_colors: int = 10, 
                       color_type: str = "primary") -> List[str]:
        """Create gradient between two team colors.
        
        Args:
            team1: First team abbreviation
            team2: Second team abbreviation  
            n_colors: Number of colors in gradient
            color_type: Type of color to use
            
        Returns:
            List of hex color codes forming gradient
        """
        color1 = self.get_team_colors(team1, color_type)
        color2 = self.get_team_colors(team2, color_type)
        
        # Convert to RGB
        rgb1 = mcolors.hex2color(color1)
        rgb2 = mcolors.hex2color(color2)
        
        # Create gradient
        gradient_colors = []
        for i in range(n_colors):
            ratio = i / (n_colors - 1)
            rgb = tuple(rgb1[j] * (1 - ratio) + rgb2[j] * ratio for j in range(3))
            gradient_colors.append(mcolors.rgb2hex(rgb))
        
        return gradient_colors
    
    def create_conference_palette(self, conference: str = "NFL", 
                                 color_type: str = "primary") -> List[str]:
        """Create color palette for all teams in a conference.
        
        Args:
            conference: 'AFC', 'NFC', or 'NFL' (both conferences)
            color_type: Type of color to use
            
        Returns:
            List of hex color codes
        """
        from .logos import get_conference_teams, get_available_teams
        
        if conference.upper() == "NFL":
            teams = get_available_teams()
        else:
            teams = get_conference_teams(conference)
        
        return self.create_team_palette(teams, color_type)
    
    def create_division_palette(self, division: str, 
                               color_type: str = "primary") -> List[str]:
        """Create color palette for teams in a division.
        
        Args:
            division: Division name (e.g., 'AFC East', 'NFC West')
            color_type: Type of color to use
            
        Returns:
            List of hex color codes
            
        Note:
            This is a simplified implementation. Real implementation would
            need to load division mappings from nfl_data_py.
        """
        # Simplified division mappings - in production, load from data
        divisions = {
            'AFC EAST': ['BUF', 'MIA', 'NE', 'NYJ'],
            'AFC NORTH': ['BAL', 'CIN', 'CLE', 'PIT'],
            'AFC SOUTH': ['HOU', 'IND', 'JAC', 'TEN'],
            'AFC WEST': ['DEN', 'KC', 'LV', 'LAC'],
            'NFC EAST': ['DAL', 'NYG', 'PHI', 'WAS'],
            'NFC NORTH': ['CHI', 'DET', 'GB', 'MIN'],
            'NFC SOUTH': ['ATL', 'CAR', 'NO', 'TB'],
            'NFC WEST': ['ARI', 'LAR', 'SEA', 'SF']
        }
        
        division = division.upper()
        if division not in divisions:
            raise ValueError(f"Invalid division: {division}")
        
        return self.create_team_palette(divisions[division], color_type)
    
    def get_contrasting_color(self, team: str, 
                             background: str = "white") -> str:
        """Get contrasting color for text over team color.
        
        Args:
            team: Team abbreviation
            background: Background context ('white', 'black', 'auto')
            
        Returns:
            Hex color code for contrasting text
        """
        primary_color = self.get_team_colors(team, "primary")
        
        # Convert to RGB for luminance calculation
        rgb = mcolors.hex2color(primary_color)
        
        # Calculate relative luminance (simplified)
        luminance = 0.299 * rgb[0] + 0.587 * rgb[1] + 0.114 * rgb[2]
        
        if background == "auto":
            # Return white for dark colors, black for light colors
            return "#ffffff" if luminance < 0.5 else "#000000"
        elif background == "white":
            # Return team's secondary or black for white background
            return self.get_team_colors(team, "secondary")
        else:  # black background
            # Return white or team's tertiary for black background
            return "#ffffff"
    
    def to_matplotlib_colormap(self, teams: List[str], 
                              color_type: str = "primary",
                              name: str = "nfl_teams") -> mcolors.ListedColormap:
        """Create matplotlib colormap from team colors.
        
        Args:
            teams: List of team abbreviations
            color_type: Type of color to use
            name: Name for the colormap
            
        Returns:
            matplotlib ListedColormap
        """
        colors = self.create_team_palette(teams, color_type)
        return mcolors.ListedColormap(colors, name=name)
    
    def to_plotly_colorscale(self, teams: List[str],
                            color_type: str = "primary") -> List[List]:
        """Create plotly colorscale from team colors.
        
        Args:
            teams: List of team abbreviations  
            color_type: Type of color to use
            
        Returns:
            Plotly colorscale format [[position, color], ...]
        """
        colors = self.create_team_palette(teams, color_type)
        n_colors = len(colors)
        
        colorscale = []
        for i, color in enumerate(colors):
            position = i / (n_colors - 1) if n_colors > 1 else 0
            colorscale.append([position, color])
        
        return colorscale


# Singleton palette manager for easy access
_palette_manager: Optional[NFLColorPalette] = None

def get_palette_manager() -> NFLColorPalette:
    """Get the singleton palette manager instance."""
    global _palette_manager
    if _palette_manager is None:
        _palette_manager = NFLColorPalette()
    return _palette_manager


def get_team_colors(teams: Union[str, List[str]], 
                   color_type: str = "primary") -> Union[str, List[str]]:
    """Get colors for specified teams.
    
    Convenience function using singleton palette manager.
    
    Args:
        teams: Team abbreviation(s)
        color_type: Type of color ('primary', 'secondary', 'tertiary', 'quaternary')
        
    Returns:
        Color hex code(s)
    """
    manager = get_palette_manager()
    return manager.get_team_colors(teams, color_type)


def create_nfl_colormap(teams: List[str], 
                       color_type: str = "primary") -> mcolors.ListedColormap:
    """Create matplotlib colormap from NFL team colors.
    
    Args:
        teams: List of team abbreviations
        color_type: Type of color to use
        
    Returns:
        matplotlib ListedColormap
    """
    manager = get_palette_manager()
    return manager.to_matplotlib_colormap(teams, color_type)