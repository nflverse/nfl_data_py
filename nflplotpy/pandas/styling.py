"""NFL table styling for pandas DataFrames.

Provides equivalent functionality to R's gt_nfl_logos, gt_nfl_headshots, etc.
for creating NFL-themed tables with team logos, player headshots, and styling.
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Union, Dict, Any, Tuple
import base64
from io import BytesIO
from PIL import Image
import warnings

from ..core.logos import get_team_logo, get_asset_manager, get_available_teams
from ..core.colors import get_team_colors
from ..core.utils import validate_teams


class NFLTableStyler:
    """Advanced styler for NFL-themed pandas tables."""
    
    def __init__(self, df: pd.DataFrame):
        """Initialize with a DataFrame.
        
        Args:
            df: pandas DataFrame to style
        """
        self.df = df.copy()
        self.styler = df.style
        
    def with_team_logos(self, columns: Union[str, List[str]], 
                       logo_height: int = 25,
                       replace_text: bool = True) -> 'NFLTableStyler':
        """Add team logos to specified columns.
        
        Args:
            columns: Column name(s) containing team abbreviations
            logo_height: Height of logos in pixels
            replace_text: If True, replace text with logo; if False, add logo alongside
            
        Returns:
            Self for method chaining
        """
        if isinstance(columns, str):
            columns = [columns]
            
        for col in columns:
            if col not in self.df.columns:
                warnings.warn(f"Column '{col}' not found in DataFrame")
                continue
                
            # Apply logo formatting
            self.styler = self.styler.format(
                self._logo_formatter(logo_height, replace_text),
                subset=[col]
            )
            
        return self
        
    def with_team_colors(self, columns: Union[str, List[str]], 
                        team_column: str,
                        color_type: str = "primary",
                        apply_to: str = "background") -> 'NFLTableStyler':
        """Apply team colors to specified columns.
        
        Args:
            columns: Column name(s) to apply colors to
            team_column: Column containing team abbreviations
            color_type: Type of team color to use
            apply_to: Where to apply color ('background', 'text')
            
        Returns:
            Self for method chaining
        """
        if isinstance(columns, str):
            columns = [columns]
            
        if team_column not in self.df.columns:
            warnings.warn(f"Team column '{team_column}' not found")
            return self
            
        # Create color mapping
        teams = self.df[team_column].unique()
        valid_teams = []
        for team in teams:
            try:
                validate_teams(str(team))
                valid_teams.append(str(team))
            except ValueError:
                continue
                
        if not valid_teams:
            warnings.warn("No valid team abbreviations found")
            return self
            
        colors = get_team_colors(valid_teams, color_type)
        color_map = dict(zip(valid_teams, colors))
        
        def color_styler(row):
            team = str(row[team_column])
            if team in color_map:
                color = color_map[team]
                if apply_to == "background":
                    return [f'background-color: {color}' if col in columns else '' 
                           for col in row.index]
                else:  # text
                    return [f'color: {color}' if col in columns else '' 
                           for col in row.index]
            return ['' for _ in row.index]
            
        self.styler = self.styler.apply(color_styler, axis=1)
        return self
        
    def with_nfl_theme(self, alternating_rows: bool = True,
                      header_style: str = "default") -> 'NFLTableStyler':
        """Apply NFL-themed styling to the entire table.
        
        Args:
            alternating_rows: Whether to alternate row colors
            header_style: Header styling ('default', 'bold', 'minimal')
            
        Returns:
            Self for method chaining
        """
        # Base styling
        styles = [
            {
                'selector': 'th',
                'props': [
                    ('background-color', '#013369'),  # NFL blue
                    ('color', 'white'),
                    ('font-weight', 'bold'),
                    ('text-align', 'center'),
                    ('border', '1px solid #ddd'),
                    ('padding', '8px')
                ]
            },
            {
                'selector': 'td',
                'props': [
                    ('text-align', 'center'),
                    ('border', '1px solid #ddd'),
                    ('padding', '8px'),
                    ('vertical-align', 'middle')
                ]
            },
            {
                'selector': 'table',
                'props': [
                    ('border-collapse', 'collapse'),
                    ('margin', '25px 0'),
                    ('font-family', 'Arial, sans-serif'),
                    ('min-width', '400px'),
                    ('border-radius', '5px 5px 0 0'),
                    ('overflow', 'hidden')
                ]
            }
        ]
        
        if alternating_rows:
            styles.append({
                'selector': 'tr:nth-of-type(even)',
                'props': [('background-color', '#f2f2f2')]
            })
            
        # Apply styles
        for style in styles:
            self.styler = self.styler.set_table_styles([style], overwrite=False)
            
        return self
        
    def _logo_formatter(self, logo_height: int, replace_text: bool):
        """Create formatter function for team logos."""
        
        def format_cell(team_abbr):
            if pd.isna(team_abbr):
                return team_abbr
                
            try:
                team_abbr = str(team_abbr).upper()
                validate_teams(team_abbr)
                
                # Get logo image
                logo_img = get_team_logo(team_abbr)
                
                # Resize to specified height, maintaining aspect ratio
                aspect_ratio = logo_img.width / logo_img.height
                new_width = int(logo_height * aspect_ratio)
                logo_img = logo_img.resize((new_width, logo_height), Image.Resampling.LANCZOS)
                
                # Convert to base64 for HTML embedding
                img_buffer = BytesIO()
                logo_img.save(img_buffer, format='PNG')
                img_str = base64.b64encode(img_buffer.getvalue()).decode()
                
                # Create HTML img tag
                if replace_text:
                    return f'<img src="data:image/png;base64,{img_str}" height="{logo_height}px" alt="{team_abbr}">'
                else:
                    return f'{team_abbr} <img src="data:image/png;base64,{img_str}" height="{logo_height}px" alt="{team_abbr}">'
                    
            except Exception as e:
                warnings.warn(f"Could not load logo for {team_abbr}: {e}")
                return team_abbr
                
        return format_cell
        
    def to_html(self, **kwargs) -> str:
        """Export to HTML string.
        
        Args:
            **kwargs: Arguments passed to pandas Styler.to_html()
            
        Returns:
            HTML string representation
        """
        return self.styler.to_html(escape=False, **kwargs)
        
    def save_html(self, filename: str, **kwargs):
        """Save to HTML file.
        
        Args:
            filename: Output filename
            **kwargs: Arguments passed to to_html()
        """
        html_content = self.to_html(**kwargs)
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)


def style_with_logos(df: pd.DataFrame, 
                    team_columns: Union[str, List[str]],
                    logo_height: int = 25,
                    replace_text: bool = True) -> pd.io.formats.style.Styler:
    """Add team logos to DataFrame columns.
    
    Equivalent to R's gt_nfl_logos().
    
    Args:
        df: pandas DataFrame
        team_columns: Column name(s) containing team abbreviations
        logo_height: Height of logos in pixels  
        replace_text: If True, replace text with logo
        
    Returns:
        pandas Styler with logo formatting
        
    Example:
        >>> df = pd.DataFrame({'team': ['KC', 'BUF'], 'wins': [14, 13]})
        >>> styled = style_with_logos(df, 'team')
        >>> styled.to_html('nfl_table.html')
    """
    styler = NFLTableStyler(df)
    return styler.with_team_logos(team_columns, logo_height, replace_text).styler


def style_with_headshots(df: pd.DataFrame,
                        player_columns: Union[str, List[str]], 
                        headshot_height: int = 30) -> pd.io.formats.style.Styler:
    """Add player headshots to DataFrame columns.
    
    Equivalent to R's gt_nfl_headshots().
    
    Args:
        df: pandas DataFrame
        player_columns: Column name(s) containing player IDs/names
        headshot_height: Height of headshots in pixels
        
    Returns:
        pandas Styler with headshot formatting
        
    Note:
        Currently uses placeholder implementation pending headshot URL system.
    """
    warnings.warn("Player headshots not yet fully implemented - using placeholders")
    
    if isinstance(player_columns, str):
        player_columns = [player_columns]
        
    styler = df.style
    
    def headshot_formatter(player_id):
        if pd.isna(player_id):
            return player_id
        # Placeholder implementation
        return f'👤 {player_id}'  # Using emoji as placeholder
        
    for col in player_columns:
        if col in df.columns:
            styler = styler.format(headshot_formatter, subset=[col])
            
    return styler


def style_with_wordmarks(df: pd.DataFrame,
                        team_columns: Union[str, List[str]],
                        wordmark_height: int = 20) -> pd.io.formats.style.Styler:
    """Add team wordmarks to DataFrame columns.
    
    Equivalent to R's gt_nfl_wordmarks().
    
    Args:
        df: pandas DataFrame  
        team_columns: Column name(s) containing team abbreviations
        wordmark_height: Height of wordmarks in pixels
        
    Returns:
        pandas Styler with wordmark formatting
        
    Note:
        Currently uses placeholder implementation pending wordmark URL system.
    """
    warnings.warn("Team wordmarks not yet fully implemented - using team names")
    
    if isinstance(team_columns, str):
        team_columns = [team_columns]
        
    # Team name mapping for wordmark placeholders
    team_names = {
        'ARI': 'CARDINALS', 'ATL': 'FALCONS', 'BAL': 'RAVENS', 'BUF': 'BILLS',
        'CAR': 'PANTHERS', 'CHI': 'BEARS', 'CIN': 'BENGALS', 'CLE': 'BROWNS',
        'DAL': 'COWBOYS', 'DEN': 'BRONCOS', 'DET': 'LIONS', 'GB': 'PACKERS',
        'HOU': 'TEXANS', 'IND': 'COLTS', 'JAC': 'JAGUARS', 'KC': 'CHIEFS',
        'LV': 'RAIDERS', 'LAC': 'CHARGERS', 'LAR': 'RAMS', 'MIA': 'DOLPHINS',
        'MIN': 'VIKINGS', 'NE': 'PATRIOTS', 'NO': 'SAINTS', 'NYG': 'GIANTS',
        'NYJ': 'JETS', 'PHI': 'EAGLES', 'PIT': 'STEELERS', 'SEA': 'SEAHAWKS',
        'SF': '49ERS', 'TB': 'BUCCANEERS', 'TEN': 'TITANS', 'WAS': 'COMMANDERS'
    }
    
    styler = df.style
    
    def wordmark_formatter(team_abbr):
        if pd.isna(team_abbr):
            return team_abbr
        team_abbr = str(team_abbr).upper()
        return team_names.get(team_abbr, team_abbr)
        
    for col in team_columns:
        if col in df.columns:
            styler = styler.format(wordmark_formatter, subset=[col])
            
    return styler


def create_nfl_table(df: pd.DataFrame,
                    team_column: Optional[str] = None,
                    logo_columns: Optional[Union[str, List[str]]] = None,
                    color_columns: Optional[Union[str, List[str]]] = None,
                    title: Optional[str] = None,
                    **kwargs) -> NFLTableStyler:
    """Create a comprehensive NFL-styled table.
    
    High-level function for creating professional NFL tables with logos,
    colors, and styling.
    
    Args:
        df: pandas DataFrame
        team_column: Column containing team abbreviations
        logo_columns: Columns to add logos to (defaults to team_column)
        color_columns: Columns to apply team colors to
        title: Table title
        **kwargs: Additional styling arguments
        
    Returns:
        NFLTableStyler for further customization
        
    Example:
        >>> standings = pd.DataFrame({
        ...     'team': ['KC', 'BUF', 'CIN'],
        ...     'wins': [14, 13, 12],
        ...     'losses': [3, 4, 5]
        ... })
        >>> table = create_nfl_table(
        ...     standings, 
        ...     team_column='team',
        ...     title='2024 AFC Standings'
        ... )
        >>> table.save_html('standings.html')
    """
    styler = NFLTableStyler(df)
    
    # Apply NFL theme
    styler = styler.with_nfl_theme(**kwargs)
    
    if team_column and team_column in df.columns:
        # Add logos
        if logo_columns is None:
            logo_columns = team_column
        if isinstance(logo_columns, str):
            logo_columns = [logo_columns]
            
        for col in logo_columns:
            if col in df.columns:
                styler = styler.with_team_logos(col)
        
        # Add team colors  
        if color_columns:
            styler = styler.with_team_colors(color_columns, team_column)
    
    return styler