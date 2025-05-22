#!/usr/bin/env python3
"""
Comprehensive validation of climate averages for all regions.
"""

import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import os
from datetime import datetime

# Define region-specific map projections and bounds
REGION_CONFIGS = {
    'conus': {
        'name': 'Continental US',
        'projection': ccrs.AlbersEqualArea(central_longitude=-96, central_latitude=38),
        'extent': [-126, -66, 24, 50]
    },
    'alaska': {
        'name': 'Alaska',
        'projection': ccrs.AlbersEqualArea(central_longitude=-154, central_latitude=60),
        'extent': [-170, -130, 50, 72]
    },
    'hawaii_and_islands': {
        'name': 'Hawaii and Islands',
        'projection': ccrs.AlbersEqualArea(central_longitude=-157, central_latitude=20),
        'extent': [-178, -155, 18, 29]
    },
    'puerto_rico_and_u.s._virgin_islands': {
        'name': 'Puerto Rico and U.S. Virgin Islands',
        'projection': ccrs.PlateCarree(),
        'extent': [-68, -64, 17, 19]
    },
    'guam_and_northern_mariana_islands': {
        'name': 'Guam and Northern Mariana Islands',
        'projection': ccrs.PlateCarree(),
        'extent': [144, 147, 13, 21]
    }
}

# Define scenarios to validate
SCENARIOS = ['historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585']

def plot_region_temperature_variables(ds, region_key, period, output_dir):
    """Plot temperature variables for a specific region."""
    config = REGION_CONFIGS[region_key]
    fig = plt.figure(figsize=(20, 15))
    fig.suptitle(f'Temperature Variables - {config["name"]} - {period}', fontsize=16)
    
    # Plot mean, max, min temperatures
    variables = {
        'tas': 'Mean Temperature',
        'tasmax': 'Maximum Temperature',
        'tasmin': 'Minimum Temperature'
    }
    
    for i, (var, title) in enumerate(variables.items(), 1):
        ax = fig.add_subplot(2, 2, i, projection=config['projection'])
        try:
            mean_temp = ds[f'{var}_{period}'].mean(dim='time')
            mean_temp.plot(ax=ax, transform=ccrs.PlateCarree(), cmap='RdBu_r')
            ax.set_extent(config['extent'], crs=ccrs.PlateCarree())
            ax.add_feature(cfeature.COASTLINE)
            ax.add_feature(cfeature.STATES, linestyle=':')
            ax.set_title(f'{title} (°C)')
        except KeyError:
            ax.text(0.5, 0.5, f'No {var} data available',
                   ha='center', va='center', transform=ax.transAxes)
    
    # Plot temperature range
    ax = fig.add_subplot(2, 2, 4, projection=config['projection'])
    try:
        temp_range = ds[f'tasmax_{period}'].mean(dim='time') - ds[f'tasmin_{period}'].mean(dim='time')
        temp_range.plot(ax=ax, transform=ccrs.PlateCarree(), cmap='RdBu_r')
        ax.set_extent(config['extent'], crs=ccrs.PlateCarree())
        ax.add_feature(cfeature.COASTLINE)
        ax.add_feature(cfeature.STATES, linestyle=':')
        ax.set_title('Temperature Range (°C)')
    except KeyError:
        ax.text(0.5, 0.5, 'Temperature range not available',
               ha='center', va='center', transform=ax.transAxes)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'{region_key}_{period}_temperature.png'))
    plt.close()

def plot_region_extremes(ds, region_key, period, output_dir):
    """Plot extreme indices for a specific region."""
    config = REGION_CONFIGS[region_key]
    fig = plt.figure(figsize=(20, 15))
    fig.suptitle(f'Climate Extremes - {config["name"]} - {period}', fontsize=16)
    
    # Define extreme indices
    extremes = {
        1: {'var': 'tasmax', 'threshold': 32.22, 'op': 'gt',
            'title': 'Days Above 90°F per Year', 'cmap': 'YlOrRd'},
        2: {'var': 'tasmin', 'threshold': 0, 'op': 'lt',
            'title': 'Frost Days per Year', 'cmap': 'YlOrRd'},
        3: {'var': 'pr', 'threshold': 25.4, 'op': 'gt',
            'title': 'Days with >1 inch Precipitation per Year', 'cmap': 'YlGnBu'},
        4: {'var': 'pr', 'threshold': None, 'op': 'mean',
            'title': 'Mean Precipitation (mm/day)', 'cmap': 'YlGnBu'}
    }
    
    for pos, extreme in extremes.items():
        ax = fig.add_subplot(2, 2, pos, projection=config['projection'])
        try:
            if extreme['op'] == 'mean':
                data = ds[f'{extreme["var"]}_{period}'].mean(dim='time')
            else:
                op = np.greater if extreme['op'] == 'gt' else np.less
                data = (op(ds[f'{extreme["var"]}_{period}'], extreme['threshold'])
                       .mean(dim='time') * 365)
            
            data.plot(ax=ax, transform=ccrs.PlateCarree(), cmap=extreme['cmap'])
            ax.set_extent(config['extent'], crs=ccrs.PlateCarree())
            ax.add_feature(cfeature.COASTLINE)
            ax.add_feature(cfeature.STATES, linestyle=':')
            ax.set_title(extreme['title'])
        except KeyError:
            ax.text(0.5, 0.5, f'No {extreme["var"]} data available',
                   ha='center', va='center', transform=ax.transAxes)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'{region_key}_{period}_extremes.png'))
    plt.close()

def print_region_statistics(ds, region_key, period):
    """Print statistics for a specific region."""
    config = REGION_CONFIGS[region_key]
    print(f"\nStatistics for {config['name']} - {period}")
    print("=" * 60)
    
    # Temperature Statistics
    for var in ['tas', 'tasmax', 'tasmin']:
        try:
            data = ds[f'{var}_{period}']
            spatial_mean = data.mean(dim=['lat', 'lon'])
            
            print(f"\n{var.upper()} Statistics:")
            print(f"Annual Mean: {spatial_mean.mean().values:.1f}°C")
            print("Seasonal Means:")
            print(f"  Winter (DJF): {spatial_mean[0:59].mean().values:.1f}°C")
            print(f"  Spring (MAM): {spatial_mean[59:151].mean().values:.1f}°C")
            print(f"  Summer (JJA): {spatial_mean[151:243].mean().values:.1f}°C")
            print(f"  Fall (SON): {spatial_mean[243:334].mean().values:.1f}°C")
        except KeyError:
            print(f"\n{var.upper()} data not available")
    
    # Precipitation Statistics
    try:
        pr_data = ds[f'pr_{period}']
        spatial_mean = pr_data.mean(dim=['lat', 'lon'])
        print("\nPrecipitation Statistics:")
        print(f"Annual Mean: {spatial_mean.mean().values:.1f} mm/day")
        print(f"Annual Total: {(spatial_mean.mean().values * 365):.1f} mm/year")
        print("Seasonal Means:")
        print(f"  Winter (DJF): {spatial_mean[0:59].mean().values:.1f} mm/day")
        print(f"  Spring (MAM): {spatial_mean[59:151].mean().values:.1f} mm/day")
        print(f"  Summer (JJA): {spatial_mean[151:243].mean().values:.1f} mm/day")
        print(f"  Fall (SON): {spatial_mean[243:334].mean().values:.1f} mm/day")
    except KeyError:
        print("\nPrecipitation data not available")
    
    # Extreme Events Statistics
    print("\nExtreme Events Statistics (Annual Days):")
    try:
        hot_days = (ds[f'tasmax_{period}'] > 32.22).mean(dim=['lat', 'lon']).mean() * 365
        print(f"Hot Days (>90°F): {hot_days.values:.1f}")
    except KeyError:
        print("Hot days statistics not available")
    
    try:
        frost_days = (ds[f'tasmin_{period}'] < 0).mean(dim=['lat', 'lon']).mean() * 365
        print(f"Frost Days (<32°F): {frost_days.values:.1f}")
    except KeyError:
        print("Frost days statistics not available")
    
    try:
        heavy_pr_days = (ds[f'pr_{period}'] > 25.4).mean(dim=['lat', 'lon']).mean() * 365
        print(f"Heavy Precipitation Days (>1 inch): {heavy_pr_days.values:.1f}")
    except KeyError:
        print("Heavy precipitation days statistics not available")

def validate_region(region_key, scenario, output_dir):
    """Validate climate averages for a specific region and scenario."""
    config = REGION_CONFIGS[region_key]
    print(f"\nValidating {config['name']} - {scenario}...")
    
    # Load the data
    try:
        # Construct filename - historical is a special case
        if scenario == 'historical':
            filename = f'{region_key}_historical_climate_averages.nc'
            period = '2012-2014'  # Historical period
        else:
            filename = f'{region_key}_{scenario}_climate_averages.nc'
            period = scenario  # For future scenarios
            
        ds = xr.open_dataset(f'output/climate_means/{filename}')
        
        # Generate validation plots
        plot_region_temperature_variables(ds, region_key, period, output_dir)
        plot_region_extremes(ds, region_key, period, output_dir)
        
        # Print statistics
        print_region_statistics(ds, region_key, period)
        
    except FileNotFoundError:
        print(f"No data file found for {region_key} - {scenario}")
        return

def main():
    """Main validation function."""
    output_dir = 'output/climate_means/validation'
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each region and scenario
    for region_key in REGION_CONFIGS.keys():
        for scenario in SCENARIOS:
            validate_region(region_key, scenario, output_dir)
    
    print("\nValidation complete. Results saved in:", output_dir)

if __name__ == "__main__":
    main() 