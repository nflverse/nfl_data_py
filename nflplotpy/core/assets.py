"""Asset management for NFL logos, headshots, and other graphics."""

import os
import requests
from pathlib import Path
from typing import Optional, Dict, Any
import appdirs
from PIL import Image
import io


class NFLAssetManager:
    """Manages downloading and caching of NFL assets including logos, headshots, and wordmarks."""
    
    def __init__(self, cache_dir: Optional[str] = None):
        """Initialize the asset manager.
        
        Args:
            cache_dir: Directory to cache assets. If None, uses user cache directory.
        """
        if cache_dir is None:
            cache_dir = appdirs.user_cache_dir("nflplotpy")
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Asset subdirectories
        self.logos_dir = self.cache_dir / "logos"
        self.headshots_dir = self.cache_dir / "headshots"
        self.wordmarks_dir = self.cache_dir / "wordmarks"
        
        for dir_path in [self.logos_dir, self.headshots_dir, self.wordmarks_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def _download_image(self, url: str, cache_path: Path, timeout: int = 30) -> Image.Image:
        """Download an image from URL and cache it locally.
        
        Args:
            url: URL to download from
            cache_path: Local path to cache the image
            timeout: Request timeout in seconds
            
        Returns:
            PIL Image object
            
        Raises:
            requests.RequestException: If download fails
            PIL.UnidentifiedImageError: If image cannot be opened
        """
        try:
            # Set proper headers to comply with Wikipedia User-Agent policy
            headers = {
                'User-Agent': 'nflplotpy/0.1.0 (https://github.com/nflverse/nfl_data_py; nflplotpy@nflverse.com) Python/3.x',
                'Accept': 'image/*,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            # Save to cache
            with open(cache_path, 'wb') as f:
                f.write(response.content)
            
            # Return PIL Image
            return Image.open(io.BytesIO(response.content))
            
        except requests.RequestException as e:
            raise requests.RequestException(f"Failed to download image from {url}: {e}")
        except Exception as e:
            raise Exception(f"Failed to process image from {url}: {e}")
    
    def _load_cached_image(self, cache_path: Path) -> Image.Image:
        """Load image from cache.
        
        Args:
            cache_path: Path to cached image
            
        Returns:
            PIL Image object
        """
        return Image.open(cache_path)
    
    def get_logo(self, team: str, format: str = "png", force_refresh: bool = False) -> Image.Image:
        """Get NFL team logo.
        
        Args:
            team: Team abbreviation (e.g., 'ARI', 'ATL')
            format: Image format ('png', 'svg')
            force_refresh: If True, re-download even if cached
            
        Returns:
            PIL Image object
            
        Raises:
            ValueError: If team abbreviation is not valid
            Exception: If logo cannot be retrieved
        """
        from .logos import NFL_TEAM_LOGOS
        
        team = team.upper()
        if team not in NFL_TEAM_LOGOS:
            raise ValueError(f"Invalid team abbreviation: {team}")
        
        cache_filename = f"{team}_logo.{format}"
        cache_path = self.logos_dir / cache_filename
        
        # Check if cached and not forcing refresh
        if cache_path.exists() and not force_refresh:
            return self._load_cached_image(cache_path)
        
        # Download from URL
        logo_url = NFL_TEAM_LOGOS[team]
        return self._download_image(logo_url, cache_path)
    
    def get_headshot(self, player_id: str, force_refresh: bool = False) -> Image.Image:
        """Get NFL player headshot.
        
        Args:
            player_id: Player ID or name
            force_refresh: If True, re-download even if cached
            
        Returns:
            PIL Image object
            
        Note:
            Currently uses placeholder implementation. 
            Real implementation would need player headshot URLs.
        """
        cache_filename = f"{player_id}_headshot.png"
        cache_path = self.headshots_dir / cache_filename
        
        # Check if cached and not forcing refresh
        if cache_path.exists() and not force_refresh:
            return self._load_cached_image(cache_path)
        
        # TODO: Implement actual headshot URLs
        # For now, create a placeholder
        placeholder = Image.new('RGBA', (100, 100), (128, 128, 128, 128))
        placeholder.save(cache_path)
        return placeholder
    
    def get_wordmark(self, team: str, force_refresh: bool = False) -> Image.Image:
        """Get NFL team wordmark.
        
        Args:
            team: Team abbreviation (e.g., 'ARI', 'ATL')  
            force_refresh: If True, re-download even if cached
            
        Returns:
            PIL Image object
            
        Note:
            Currently uses placeholder implementation.
            Real implementation would need wordmark URLs.
        """
        team = team.upper()
        cache_filename = f"{team}_wordmark.png"
        cache_path = self.wordmarks_dir / cache_filename
        
        # Check if cached and not forcing refresh  
        if cache_path.exists() and not force_refresh:
            return self._load_cached_image(cache_path)
        
        # TODO: Implement actual wordmark URLs
        # For now, create a placeholder
        placeholder = Image.new('RGBA', (200, 50), (128, 128, 128, 128))
        placeholder.save(cache_path)
        return placeholder
    
    def clear_cache(self, asset_type: Optional[str] = None) -> None:
        """Clear cached assets.
        
        Args:
            asset_type: Type of assets to clear ('logos', 'headshots', 'wordmarks').
                       If None, clears all assets.
        """
        if asset_type is None:
            # Clear all
            for dir_path in [self.logos_dir, self.headshots_dir, self.wordmarks_dir]:
                for file_path in dir_path.glob("*"):
                    if file_path.is_file():
                        file_path.unlink()
        elif asset_type == "logos":
            for file_path in self.logos_dir.glob("*"):
                if file_path.is_file():
                    file_path.unlink()
        elif asset_type == "headshots":
            for file_path in self.headshots_dir.glob("*"):
                if file_path.is_file():
                    file_path.unlink()
        elif asset_type == "wordmarks":
            for file_path in self.wordmarks_dir.glob("*"):
                if file_path.is_file():
                    file_path.unlink()
        else:
            raise ValueError(f"Invalid asset_type: {asset_type}")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get information about cached assets.
        
        Returns:
            Dictionary with cache statistics
        """
        def count_files(directory: Path) -> int:
            return len([f for f in directory.glob("*") if f.is_file()])
        
        def get_directory_size(directory: Path) -> int:
            return sum(f.stat().st_size for f in directory.glob("*") if f.is_file())
        
        return {
            "cache_dir": str(self.cache_dir),
            "logos_count": count_files(self.logos_dir),
            "headshots_count": count_files(self.headshots_dir), 
            "wordmarks_count": count_files(self.wordmarks_dir),
            "total_size_bytes": (
                get_directory_size(self.logos_dir) +
                get_directory_size(self.headshots_dir) +
                get_directory_size(self.wordmarks_dir)
            )
        }