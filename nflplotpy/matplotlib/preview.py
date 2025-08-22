"""Plot preview functionality for matplotlib.

Equivalent to R's ggpreview() function for previewing plots
in specified dimensions and settings.
"""

import matplotlib.pyplot as plt
import matplotlib.figure
from typing import Tuple, Optional, Union, Any
import tempfile
import os
import numpy as np
from pathlib import Path
import warnings

try:
    from IPython.display import Image as IPythonImage, display
    HAS_IPYTHON = True
except ImportError:
    HAS_IPYTHON = False


def nfl_preview(fig: Optional[matplotlib.figure.Figure] = None,
               width: float = 8,
               height: float = 6,
               dpi: int = 100,
               format: str = 'png',
               show_in_notebook: bool = True,
               save_path: Optional[str] = None,
               **kwargs) -> Optional[str]:
    """Preview matplotlib figure with specified dimensions and settings.
    
    Equivalent to R's ggpreview() function.
    
    Args:
        fig: matplotlib Figure to preview. If None, uses current figure.
        width: Figure width in inches
        height: Figure height in inches  
        dpi: Resolution in dots per inch
        format: Output format ('png', 'jpg', 'pdf', 'svg')
        show_in_notebook: Whether to display in Jupyter notebook
        save_path: Path to save preview file. If None, uses temporary file.
        **kwargs: Additional arguments passed to fig.savefig()
        
    Returns:
        Path to saved preview file, or None if displayed in notebook
        
    Example:
        >>> import matplotlib.pyplot as plt
        >>> fig, ax = plt.subplots()
        >>> ax.plot([1, 2, 3], [1, 4, 2])
        >>> nfl_preview(fig, width=10, height=6, dpi=150)
    """
    # Get figure
    if fig is None:
        fig = plt.gcf()
        if not fig.axes:
            warnings.warn("No active figure found. Create a plot first.")
            return None
    
    # Set figure size
    original_size = fig.get_size_inches()
    fig.set_size_inches(width, height)
    
    try:
        # Determine save path
        if save_path is None:
            # Create temporary file
            temp_dir = tempfile.gettempdir()
            temp_file = tempfile.NamedTemporaryFile(
                suffix=f'.{format}',
                dir=temp_dir,
                delete=False
            )
            save_path = temp_file.name
            temp_file.close()
        
        # Default save arguments
        save_kwargs = {
            'dpi': dpi,
            'bbox_inches': 'tight',
            'facecolor': 'white',
            'edgecolor': 'none'
        }
        save_kwargs.update(kwargs)
        
        # Save figure
        fig.savefig(save_path, format=format, **save_kwargs)
        
        # Display in notebook if requested and available
        if show_in_notebook and HAS_IPYTHON:
            try:
                display(IPythonImage(save_path))
                return None  # Don't return path since we displayed inline
            except Exception:
                pass  # Fall back to returning path
        
        return save_path
        
    finally:
        # Restore original size
        fig.set_size_inches(original_size)


def preview_with_dimensions(fig: Optional[matplotlib.figure.Figure] = None,
                           dimensions: Union[str, Tuple[float, float]] = "standard",
                           **kwargs) -> Optional[str]:
    """Preview figure with predefined dimension sets.
    
    Args:
        fig: matplotlib Figure to preview
        dimensions: Predefined dimension name or (width, height) tuple
        **kwargs: Additional arguments passed to nfl_preview()
        
    Returns:
        Path to saved preview file
        
    Available dimensions:
        - "standard": 8x6 inches
        - "wide": 12x6 inches  
        - "square": 8x8 inches
        - "presentation": 16x9 inches
        - "poster": 18x24 inches
        - "social": 10x10 inches (square for social media)
    """
    predefined_dimensions = {
        "standard": (8, 6),
        "wide": (12, 6),
        "square": (8, 8), 
        "presentation": (16, 9),
        "poster": (18, 24),
        "social": (10, 10),
        "mobile": (6, 8)  # Portrait for mobile
    }
    
    if isinstance(dimensions, str):
        if dimensions not in predefined_dimensions:
            raise ValueError(f"Unknown dimension preset: {dimensions}. "
                           f"Available: {list(predefined_dimensions.keys())}")
        width, height = predefined_dimensions[dimensions]
    else:
        width, height = dimensions
    
    return nfl_preview(fig, width=width, height=height, **kwargs)


def preview_comparison(figures: list,
                      width: float = 6,
                      height: float = 4,
                      dpi: int = 100,
                      format: str = 'png') -> list:
    """Preview multiple figures side by side for comparison.
    
    Args:
        figures: List of matplotlib Figure objects
        width: Width for each figure
        height: Height for each figure
        dpi: Resolution
        format: Output format
        
    Returns:
        List of paths to preview files
    """
    if not figures:
        raise ValueError("No figures provided")
    
    paths = []
    for i, fig in enumerate(figures):
        if fig is None:
            continue
            
        temp_file = tempfile.NamedTemporaryFile(
            suffix=f'_comparison_{i}.{format}',
            delete=False
        )
        temp_file.close()
        
        path = nfl_preview(
            fig, 
            width=width, 
            height=height,
            dpi=dpi,
            format=format,
            show_in_notebook=False,
            save_path=temp_file.name
        )
        
        if path:
            paths.append(path)
    
    # Display all in notebook if available
    if HAS_IPYTHON and paths:
        try:
            from IPython.display import HTML
            html_content = '<div style="display: flex; flex-wrap: wrap;">'
            for i, path in enumerate(paths):
                html_content += f'<div style="margin: 10px;"><h4>Figure {i+1}</h4><img src="{path}" style="max-width: 300px;"></div>'
            html_content += '</div>'
            display(HTML(html_content))
        except Exception:
            pass
    
    return paths


def save_preview_grid(figures: list,
                     output_path: str,
                     grid_shape: Optional[Tuple[int, int]] = None,
                     fig_width: float = 4,
                     fig_height: float = 3,
                     dpi: int = 150,
                     **kwargs):
    """Save multiple figures as a grid layout.
    
    Args:
        figures: List of matplotlib Figure objects
        output_path: Path to save combined grid
        grid_shape: (rows, cols) for grid. If None, auto-determined.
        fig_width: Width of each subplot figure
        fig_height: Height of each subplot figure  
        dpi: Resolution
        **kwargs: Additional arguments for savefig
    """
    if not figures:
        raise ValueError("No figures provided")
    
    # Filter out None figures
    valid_figures = [fig for fig in figures if fig is not None]
    n_figs = len(valid_figures)
    
    if n_figs == 0:
        raise ValueError("No valid figures found")
    
    # Determine grid shape
    if grid_shape is None:
        cols = int(np.ceil(np.sqrt(n_figs)))
        rows = int(np.ceil(n_figs / cols))
    else:
        rows, cols = grid_shape
    
    # Create combined figure
    combined_fig = plt.figure(
        figsize=(cols * fig_width, rows * fig_height),
        dpi=dpi
    )
    
    # Save each figure to temporary files and load as images
    import matplotlib.image as mpimg
    
    for i, fig in enumerate(valid_figures):
        if i >= rows * cols:
            break
            
        # Create subplot
        ax = combined_fig.add_subplot(rows, cols, i + 1)
        
        # Save figure to temporary file
        temp_file = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
        temp_file.close()
        
        original_size = fig.get_size_inches()
        fig.set_size_inches(fig_width, fig_height)
        fig.savefig(temp_file.name, dpi=dpi, bbox_inches='tight')
        fig.set_size_inches(original_size)
        
        # Load and display image
        img = mpimg.imread(temp_file.name)
        ax.imshow(img)
        ax.axis('off')
        ax.set_title(f'Figure {i+1}', fontsize=10)
        
        # Clean up temporary file
        os.unlink(temp_file.name)
    
    # Save combined figure
    save_kwargs = {
        'dpi': dpi,
        'bbox_inches': 'tight',
        'facecolor': 'white'
    }
    save_kwargs.update(kwargs)
    
    combined_fig.savefig(output_path, **save_kwargs)
    plt.close(combined_fig)
    
    print(f"Saved preview grid to: {output_path}")


def quick_preview(plot_func, *args, **kwargs) -> Optional[str]:
    """Quick preview of a plotting function.
    
    Args:
        plot_func: Function that creates a plot
        *args: Arguments for plot_func
        **kwargs: Keyword arguments for plot_func
        
    Returns:
        Path to preview file
        
    Example:
        >>> import nflplotpy as nflplot
        >>> quick_preview(nflplot.plot_team_stats, team_data, 'epa', 'success_rate')
    """
    # Create plot
    result = plot_func(*args, **kwargs)
    
    # Get figure
    if isinstance(result, matplotlib.figure.Figure):
        fig = result
    else:
        fig = plt.gcf()
    
    # Preview
    return nfl_preview(fig, show_in_notebook=True)


# Convenience function aliases
ggpreview = nfl_preview  # R-style alias
preview = nfl_preview    # Short alias