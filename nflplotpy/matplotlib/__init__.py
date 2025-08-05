"""Matplotlib integration for nflplotpy."""

from .artists import add_nfl_logo, add_nfl_headshot, add_nfl_wordmark
from .scales import nfl_color_scale, apply_nfl_theme

__all__ = [
    "add_nfl_logo",
    "add_nfl_headshot", 
    "add_nfl_wordmark",
    "nfl_color_scale",
    "apply_nfl_theme",
]