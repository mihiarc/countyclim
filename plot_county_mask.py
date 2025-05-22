#!/usr/bin/env python3
"""
plot_county_mask.py

Plot county mask created by create_county_masks.py
"""
import os
import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import numpy as np
import cartopy.crs as ccrs
import cartopy.feature as cfeature

def plot_county_mask(mask_file, output_dir='output/figures'):
    """
    Plot county mask from NetCDF file
    
    Args:
        mask_file (str): Path to county mask NetCDF file
        output_dir (str): Directory to save the plot
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Load mask file
    print(f"Loading mask from {mask_file}")
    mask = xr.open_dataset(mask_file)
    
    # Get the mask data array (should be named 'mask')
    mask_array = mask['mask']
    
    # Create a random colormap for counties
    # Each county will have a different color
    unique_ids = np.unique(mask_array.values)
    unique_ids = unique_ids[~np.isnan(unique_ids)]  # Remove NaN
    n_counties = len(unique_ids)
    
    print(f"Found {n_counties} counties in the mask")
    
    # Set up the figure with Cartopy projection
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    
    # Add map features
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.STATES, linewidth=0.5)
    
    # Create a colormap with a distinct color for each county
    cmap = plt.cm.get_cmap('tab20c', n_counties)
    norm = colors.BoundaryNorm(np.arange(0, n_counties + 1) - 0.5, n_counties)
    
    # Plot the mask
    im = ax.pcolormesh(
        mask_array.lon, 
        mask_array.lat, 
        mask_array, 
        cmap=cmap,
        norm=norm,
        transform=ccrs.PlateCarree()
    )
    
    # Add a colorbar
    cbar = plt.colorbar(im, ax=ax, orientation='horizontal', pad=0.05, shrink=0.8)
    cbar.set_label('County ID')
    
    # Set the extent to focus on the US
    ax.set_extent([-125, -66, 24, 50], crs=ccrs.PlateCarree())
    
    # Add gridlines
    ax.gridlines(draw_labels=True)
    
    # Add title
    plt.title('County Mask')
    
    # Save the figure
    output_file = os.path.join(output_dir, 'county_mask.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved plot to {output_file}")
    
    # Show the plot
    plt.show()

if __name__ == "__main__":
    mask_file = "output/county_masks/county_mask.nc"
    plot_county_mask(mask_file) 