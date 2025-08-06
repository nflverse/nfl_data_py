"""Custom matplotlib artists for NFL logos and headshots."""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from matplotlib.artist import Artist
import numpy as np
from typing import Union, List, Tuple, Optional, Any
from PIL import Image
import io

from ..core.logos import get_team_logo, get_asset_manager
from ..core.colors import get_team_colors
from ..core.utils import validate_teams


class NFLLogoArtist(Artist):
    """Custom matplotlib artist for rendering NFL logos."""
    
    def __init__(self, team: str, xy: Tuple[float, float], 
                 width: float = 0.1, height: Optional[float] = None,
                 alpha: float = 1.0, zorder: int = 10):
        """Initialize NFL logo artist.
        
        Args:
            team: Team abbreviation
            xy: Position (x, y) in axes coordinates
            width: Logo width in axes coordinates  
            height: Logo height in axes coordinates (if None, maintains aspect ratio)
            alpha: Transparency level (0-1)
            zorder: Drawing order
        """
        super().__init__()
        self.team = team.upper()
        self.xy = xy
        self.width = width
        self.height = height
        self.alpha = alpha
        self.zorder = zorder
        self._logo_image = None
        self._annotation_bbox = None
        
    def draw(self, renderer):
        """Draw the logo on the axes."""
        if self._logo_image is None:
            self._load_logo()
        
        if self._annotation_bbox is not None:
            self._annotation_bbox.draw(renderer)
    
    def _load_logo(self):
        """Load the team logo image."""
        try:
            # Get logo image
            pil_image = get_team_logo(self.team)
            
            # Convert to numpy array for matplotlib
            image_array = np.array(pil_image)
            
            # Create OffsetImage
            offset_image = OffsetImage(image_array, zoom=self.width, alpha=self.alpha)
            
            # Create AnnotationBbox
            self._annotation_bbox = AnnotationBbox(
                offset_image, 
                self.xy,
                frameon=False,
                pad=0,
                zorder=self.zorder
            )
            
            self._logo_image = image_array
            
        except Exception as e:
            print(f"Warning: Could not load logo for {self.team}: {e}")


def add_nfl_logo(ax: plt.Axes, team: str, x: float, y: float, 
                width: float = 0.1, height: Optional[float] = None,
                alpha: float = 1.0, zorder: int = 10, 
                target_width_pixels: Optional[int] = None, **kwargs) -> AnnotationBbox:
    """Add NFL team logo to matplotlib axes.
    
    Equivalent to nflplotR's geom_nfl_logos().
    
    Args:
        ax: Matplotlib axes
        team: Team abbreviation
        x: X position
        y: Y position  
        width: Logo width (as fraction of axes width) - ignored if target_width_pixels is set
        height: Logo height (if None, maintains aspect ratio)
        alpha: Transparency level (0-1)
        zorder: Drawing order
        target_width_pixels: Target width in pixels for consistent sizing (overrides width)
        **kwargs: Additional arguments passed to AnnotationBbox
        
    Returns:
        AnnotationBbox containing the logo
        
    Raises:
        ValueError: If team abbreviation is invalid
    """
    # Validate team
    validate_teams(team)
    
    try:
        # Get logo image
        pil_image = get_team_logo(team)
        
        # For adaptive sizing, resize the PIL image first before converting to numpy
        if target_width_pixels is not None:
            # Get original dimensions
            orig_width, orig_height = pil_image.size
            
            # Calculate new height maintaining aspect ratio
            aspect_ratio = orig_height / orig_width
            new_height = int(target_width_pixels * aspect_ratio)
            
            # Resize the PIL image to exact target dimensions
            pil_image = pil_image.resize((target_width_pixels, new_height), Image.Resampling.LANCZOS)
            
            # Now use zoom=1 since we've already resized
            zoom = 1.0
        else:
            # Fallback to old method with zoom scaling
            zoom = width * 10
        
        # Convert PIL to numpy array after any resizing
        image_array = np.array(pil_image)
        
        # Create OffsetImage with calculated zoom
        offset_image = OffsetImage(image_array, zoom=zoom, alpha=alpha)
        
        # Create AnnotationBbox
        ab = AnnotationBbox(
            offset_image,
            (x, y),
            frameon=False,
            pad=0,
            zorder=zorder,
            **kwargs
        )
        
        # Add to axes
        ax.add_artist(ab)
        
        return ab
        
    except Exception as e:
        print(f"Warning: Could not add logo for {team}: {e}")
        return None


def add_nfl_logos(ax: plt.Axes, teams: List[str], x: Union[List[float], np.ndarray], 
                 y: Union[List[float], np.ndarray], width: float = 0.1, 
                 target_width_pixels: Optional[int] = None, **kwargs) -> List[AnnotationBbox]:
    """Add multiple NFL team logos to matplotlib axes.
    
    Args:
        ax: Matplotlib axes
        teams: List of team abbreviations
        x: X positions (must be same length as teams)
        y: Y positions (must be same length as teams)
        width: Logo width for all logos - ignored if target_width_pixels is set
        target_width_pixels: Target width in pixels for consistent sizing across all logos
        **kwargs: Additional arguments passed to add_nfl_logo
        
    Returns:
        List of AnnotationBbox objects
        
    Raises:
        ValueError: If arrays have different lengths
    """
    if len(teams) != len(x) or len(teams) != len(y):
        raise ValueError("teams, x, and y must have the same length")
    
    annotations = []
    for team, xi, yi in zip(teams, x, y):
        ab = add_nfl_logo(ax, team, xi, yi, width=width, target_width_pixels=target_width_pixels, **kwargs)
        if ab is not None:
            annotations.append(ab)
    
    return annotations


def add_nfl_headshot(ax: plt.Axes, player_id: str, x: float, y: float,
                    width: float = 0.1, **kwargs) -> Optional[AnnotationBbox]:
    """Add NFL player headshot to matplotlib axes.
    
    Equivalent to nflplotR's geom_nfl_headshots().
    
    Args:
        ax: Matplotlib axes
        player_id: Player ID or name
        x: X position
        y: Y position
        width: Headshot width
        **kwargs: Additional arguments
        
    Returns:
        AnnotationBbox containing the headshot
        
    Note:
        Currently uses placeholder implementation.
    """
    try:
        # Get asset manager
        manager = get_asset_manager()
        pil_image = manager.get_headshot(player_id)
        
        # Convert PIL to numpy array
        image_array = np.array(pil_image)
        
        # Calculate zoom
        zoom = width * 10
        
        # Create OffsetImage
        offset_image = OffsetImage(image_array, zoom=zoom)
        
        # Create AnnotationBbox
        ab = AnnotationBbox(
            offset_image,
            (x, y),
            frameon=False,
            pad=0,
            **kwargs
        )
        
        # Add to axes
        ax.add_artist(ab)
        
        return ab
        
    except Exception as e:
        print(f"Warning: Could not add headshot for {player_id}: {e}")
        return None


def add_nfl_wordmark(ax: plt.Axes, team: str, x: float, y: float, 
                    width: float = 0.2, **kwargs) -> Optional[AnnotationBbox]:
    """Add NFL team wordmark to matplotlib axes.
    
    Equivalent to nflplotR's geom_nfl_wordmarks().
    
    Args:
        ax: Matplotlib axes
        team: Team abbreviation
        x: X position
        y: Y position
        width: Wordmark width
        **kwargs: Additional arguments
        
    Returns:
        AnnotationBbox containing the wordmark
    """
    validate_teams(team)
    
    try:
        # Get asset manager
        manager = get_asset_manager()
        pil_image = manager.get_wordmark(team)
        
        # Convert PIL to numpy array
        image_array = np.array(pil_image)
        
        # Calculate zoom
        zoom = width * 10
        
        # Create OffsetImage
        offset_image = OffsetImage(image_array, zoom=zoom)
        
        # Create AnnotationBbox
        ab = AnnotationBbox(
            offset_image,
            (x, y),
            frameon=False,
            pad=0,
            **kwargs
        )
        
        # Add to axes
        ax.add_artist(ab)
        
        return ab
        
    except Exception as e:
        print(f"Warning: Could not add wordmark for {team}: {e}")
        return None


def add_median_lines(ax: plt.Axes, data: Union[np.ndarray, List[float]], 
                    axis: str = 'both', **kwargs):
    """Add median reference lines to plot.
    
    Equivalent to nflplotR's geom_median_lines().
    
    Args:
        ax: Matplotlib axes
        data: Data to calculate median from
        axis: Which axis to add lines ('x', 'y', 'both')
        **kwargs: Arguments passed to axhline/axvline
    """
    if isinstance(data, list):
        data = np.array(data)
    
    median_val = np.median(data)
    
    # Default line styling
    line_kwargs = {
        'color': 'red',
        'linestyle': '--',
        'alpha': 0.7,
        'linewidth': 1
    }
    line_kwargs.update(kwargs)
    
    if axis in ['y', 'both']:
        ax.axhline(y=median_val, **line_kwargs)
    
    if axis in ['x', 'both']:
        ax.axvline(x=median_val, **line_kwargs)


def add_mean_lines(ax: plt.Axes, data: Union[np.ndarray, List[float]], 
                  axis: str = 'both', **kwargs):
    """Add mean reference lines to plot.
    
    Equivalent to nflplotR's geom_mean_lines().
    
    Args:
        ax: Matplotlib axes
        data: Data to calculate mean from
        axis: Which axis to add lines ('x', 'y', 'both')
        **kwargs: Arguments passed to axhline/axvline
    """
    if isinstance(data, list):
        data = np.array(data)
    
    mean_val = np.mean(data)
    
    # Default line styling
    line_kwargs = {
        'color': 'blue',
        'linestyle': '-',
        'alpha': 0.7,
        'linewidth': 1
    }
    line_kwargs.update(kwargs)
    
    if axis in ['y', 'both']:
        ax.axhline(y=mean_val, **line_kwargs)
    
    if axis in ['x', 'both']:
        ax.axvline(x=mean_val, **line_kwargs)