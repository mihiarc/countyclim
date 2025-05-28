#!/usr/bin/env python3
"""
Additional validation of climate averages output.
"""

import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
import glob
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import os

def plot_temperature_variables(ds, scenario, output_dir):
    """Plot temperature-related variables (tas, tasmax, tasmin)."""
    fig = plt.figure(figsize=(20, 15))
    fig.suptitle(f'Temperature Variables - {scenario}', fontsize=16)
    
    # Plot 1: Mean Temperature (tas)
    ax1 = fig.add_subplot(221, projection=ccrs.PlateCarree())
    mean_tas = ds[f'tas_{scenario}'].mean(dim='time')
    mean_tas.plot(ax=ax1, transform=ccrs.PlateCarree(), cmap='RdBu_r')
    ax1.add_feature(cfeature.STATES)
    ax1.coastlines()
    ax1.set_title('Mean Temperature (°C)')
    
    # Plot 2: Maximum Temperature (tasmax)
    ax2 = fig.add_subplot(222, projection=ccrs.PlateCarree())
    mean_tasmax = ds[f'tasmax_{scenario}'].mean(dim='time')
    mean_tasmax.plot(ax=ax2, transform=ccrs.PlateCarree(), cmap='RdBu_r')
    ax2.add_feature(cfeature.STATES)
    ax2.coastlines()
    ax2.set_title('Maximum Temperature (°C)')
    
    # Plot 3: Minimum Temperature (tasmin)
    ax3 = fig.add_subplot(223, projection=ccrs.PlateCarree())
    mean_tasmin = ds[f'tasmin_{scenario}'].mean(dim='time')
    mean_tasmin.plot(ax=ax3, transform=ccrs.PlateCarree(), cmap='RdBu_r')
    ax3.add_feature(cfeature.STATES)
    ax3.coastlines()
    ax3.set_title('Minimum Temperature (°C)')
    
    # Plot 4: Temperature Range (tasmax - tasmin)
    ax4 = fig.add_subplot(224, projection=ccrs.PlateCarree())
    temp_range = mean_tasmax - mean_tasmin
    temp_range.plot(ax=ax4, transform=ccrs.PlateCarree(), cmap='RdBu_r')
    ax4.add_feature(cfeature.STATES)
    ax4.coastlines()
    ax4.set_title('Temperature Range (°C)')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'temperature_validation_{scenario}.png'))
    plt.close()

def plot_precipitation_and_extremes(ds, scenario, output_dir):
    """Plot precipitation and climate extremes."""
    fig = plt.figure(figsize=(20, 15))
    fig.suptitle(f'Precipitation and Extremes - {scenario}', fontsize=16)
    
    # Plot 1: Mean Precipitation
    ax1 = fig.add_subplot(221, projection=ccrs.PlateCarree())
    mean_pr = ds[f'pr_{scenario}'].mean(dim='time')
    mean_pr.plot(ax=ax1, transform=ccrs.PlateCarree(), cmap='YlGnBu')
    ax1.add_feature(cfeature.STATES)
    ax1.coastlines()
    ax1.set_title('Mean Precipitation (mm/day)')
    
    # Plot 2: Hot Days (tasmax > 32.22°C / 90°F)
    ax2 = fig.add_subplot(222, projection=ccrs.PlateCarree())
    hot_days = (ds[f'tasmax_{scenario}'] > 32.22).mean(dim='time') * 365
    hot_days.plot(ax=ax2, transform=ccrs.PlateCarree(), cmap='YlOrRd')
    ax2.add_feature(cfeature.STATES)
    ax2.coastlines()
    ax2.set_title('Days Above 90°F per Year')
    
    # Plot 3: Frost Days (tasmin < 0°C / 32°F)
    ax3 = fig.add_subplot(223, projection=ccrs.PlateCarree())
    frost_days = (ds[f'tasmin_{scenario}'] < 0).mean(dim='time') * 365
    frost_days.plot(ax=ax3, transform=ccrs.PlateCarree(), cmap='YlOrRd')
    ax3.add_feature(cfeature.STATES)
    ax3.coastlines()
    ax3.set_title('Frost Days per Year')
    
    # Plot 4: Heavy Precipitation Days (pr > 25.4mm / 1 inch)
    ax4 = fig.add_subplot(224, projection=ccrs.PlateCarree())
    heavy_pr_days = (ds[f'pr_{scenario}'] > 25.4).mean(dim='time') * 365
    heavy_pr_days.plot(ax=ax4, transform=ccrs.PlateCarree(), cmap='YlOrRd')
    ax4.add_feature(cfeature.STATES)
    ax4.coastlines()
    ax4.set_title('Days with >1 inch Precipitation per Year')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'extremes_validation_{scenario}.png'))
    plt.close()

def print_statistics(ds, scenario):
    """Print comprehensive statistics for all variables."""
    print(f"\nStatistics for {scenario}")
    print("=" * 50)
    
    # Temperature Statistics
    for var in ['tas', 'tasmax', 'tasmin']:
        data = ds[f'{var}_{scenario}']
        spatial_mean = data.mean(dim=['lat', 'lon'])
        
        print(f"\n{var.upper()} Statistics:")
        print(f"Annual Mean: {spatial_mean.mean().values:.1f}°C")
        print("Seasonal Means:")
        print(f"  Winter (DJF): {spatial_mean[0:59].mean().values:.1f}°C")
        print(f"  Spring (MAM): {spatial_mean[59:151].mean().values:.1f}°C")
        print(f"  Summer (JJA): {spatial_mean[151:243].mean().values:.1f}°C")
        print(f"  Fall (SON): {spatial_mean[243:334].mean().values:.1f}°C")
    
    # Precipitation Statistics
    pr_data = ds[f'pr_{scenario}']
    spatial_mean_pr = pr_data.mean(dim=['lat', 'lon'])
    print("\nPrecipitation Statistics:")
    print(f"Annual Mean: {spatial_mean_pr.mean().values:.1f} mm/day")
    print(f"Annual Total: {(spatial_mean_pr.mean().values * 365):.1f} mm/year")
    print("Seasonal Means:")
    print(f"  Winter (DJF): {spatial_mean_pr[0:59].mean().values:.1f} mm/day")
    print(f"  Spring (MAM): {spatial_mean_pr[59:151].mean().values:.1f} mm/day")
    print(f"  Summer (JJA): {spatial_mean_pr[151:243].mean().values:.1f} mm/day")
    print(f"  Fall (SON): {spatial_mean_pr[243:334].mean().values:.1f} mm/day")
    
    # Extreme Events Statistics
    print("\nExtreme Events Statistics (Annual Days):")
    hot_days = (ds[f'tasmax_{scenario}'] > 32.22).mean(dim=['lat', 'lon']).mean() * 365
    frost_days = (ds[f'tasmin_{scenario}'] < 0).mean(dim=['lat', 'lon']).mean() * 365
    heavy_pr_days = (ds[f'pr_{scenario}'] > 25.4).mean(dim=['lat', 'lon']).mean() * 365
    print(f"Hot Days (>90°F): {hot_days.values:.1f}")
    print(f"Frost Days (<32°F): {frost_days.values:.1f}")
    print(f"Heavy Precipitation Days (>1 inch): {heavy_pr_days.values:.1f}")

def main():
    """Main validation function."""
    # Create validation output directory
    output_dir = 'output/climate_means/validation'
    os.makedirs(output_dir, exist_ok=True)
    
    # Process historical data first
    try:
        ds = xr.open_dataset('output/climate_means/conus_historical_climate_averages.nc')
        period = '2012-2014'  # Historical period
        
        # Generate plots
        plot_temperature_variables(ds, period, output_dir)
        plot_precipitation_and_extremes(ds, period, output_dir)
        
        # Print statistics
        print_statistics(ds, period)
        
    except FileNotFoundError:
        print("Historical climate averages file not found")
    
    # Process future scenarios if available
    for scenario in ['ssp126', 'ssp245', 'ssp370', 'ssp585']:
        try:
            ds = xr.open_dataset(f'output/climate_means/conus_{scenario}_climate_averages.nc')
            for period in ['2050s', '2080s']:
                if f'tas_{period}' in ds:
                    plot_temperature_variables(ds, period, output_dir)
                    plot_precipitation_and_extremes(ds, period, output_dir)
                    print_statistics(ds, f"{scenario}_{period}")
        except FileNotFoundError:
            print(f"Climate averages file not found for {scenario}")

if __name__ == "__main__":
    main() 