#!/usr/bin/env python3
"""
Test script to validate precipitation unit conversions through the processing pipeline.
Creates mock precipitation data and validates unit conversions at each step.
"""

import os
import numpy as np
import xarray as xr
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
import pytest
from datetime import datetime, timedelta

# Constants for test data
TEST_PRECIP_VALUES = {
    # Test cases in kg/m²/s that should result in specific mm/day values
    'light_rain': 1.157407e-5,    # = 1 mm/day
    'moderate_rain': 2.893519e-4,  # = 25 mm/day
    'heavy_rain': 2.939815e-4,    # = 25.4 mm/day (1 inch/day)
    'very_heavy': 5.787037e-4,    # = 50 mm/day
}

def create_mock_netcdf():
    """Create a mock NetCDF file with known precipitation values."""
    # Create a 2x2 grid
    lon = np.array([270, 271])  # -90, -89 in conventional coordinates
    lat = np.array([35, 36])
    
    # Create time array for one year
    start_date = datetime(2012, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(365)]
    
    # Create precipitation data array
    # Most days will have light rain, but we'll insert known values on specific days
    precip = np.full((365, 2, 2), TEST_PRECIP_VALUES['light_rain'])
    
    # Set specific test values - use the same value for all grid cells
    # to verify that mean gives the same result regardless of number of cells
    # Day 100: moderate rain
    precip[100, :, :] = TEST_PRECIP_VALUES['moderate_rain']
    # Day 200: heavy rain (1 inch)
    precip[200, :, :] = TEST_PRECIP_VALUES['heavy_rain']
    # Day 300: very heavy rain
    precip[300, :, :] = TEST_PRECIP_VALUES['very_heavy']
    
    # Create xarray dataset
    ds = xr.Dataset(
        data_vars={
            'pr': (('time', 'lat', 'lon'), precip)
        },
        coords={
            'lon': lon,
            'lat': lat,
            'time': dates
        }
    )
    
    # Add metadata
    ds.pr.attrs['units'] = 'kg m-2 s-1'
    ds.pr.attrs['long_name'] = 'precipitation'
    
    # Save to test directory
    os.makedirs('data/pr', exist_ok=True)
    output_file = 'data/pr/pr_day_test_2012.nc'
    ds.to_netcdf(output_file)
    return output_file

def create_mock_county():
    """Create a mock county boundary that overlaps with our test data."""
    # Create two test counties:
    # 1. A county that covers just one grid cell
    # 2. A county that covers all grid cells
    geometries = [
        box(269.5, 34.5, 270.5, 35.5),  # Single cell
        box(269.5, 34.5, 271.5, 36.5)   # All cells
    ]
    
    # Create GeoDataFrame with two counties
    counties = gpd.GeoDataFrame(
        {
            'GEOID': ['01001', '01002'],
            'NAME': ['Single Cell County', 'Multi Cell County'],
            'STUSPS': ['AL', 'AL'],
            'id_numeric': [1, 2],
            'geometry': geometries
        },
        crs="EPSG:4326"
    )
    
    # Save to test directory
    os.makedirs('output/county_boundaries', exist_ok=True)
    output_file = 'output/county_boundaries/test_counties.parquet'
    counties.to_parquet(output_file)
    return output_file

def test_climate_means_conversion():
    """Test the conversion in climate_means.py."""
    from climate_means import process_period_region
    
    # Create test data
    netcdf_file = create_mock_netcdf()
    
    # Load and check original data
    ds = xr.open_dataset(netcdf_file)
    print("\nOriginal data attributes:", ds.pr.attrs)
    
    # Process the test data
    period_info = (2012, 2014, '2012-2014')
    climate_avg = process_period_region(
        period_info,
        'pr',
        'data/pr/pr_day_test_*.nc',
        'CONUS'
    )
    
    print("\nProcessed data attributes:", climate_avg.attrs)
    
    # Validate conversions
    expected_values = {
        'light_rain': 1.0,      # 1 mm/day
        'moderate_rain': 25.0,  # 25 mm/day
        'heavy_rain': 25.4,    # 25.4 mm/day (1 inch)
        'very_heavy': 50.0     # 50 mm/day
    }
    
    # Check units attribute
    assert climate_avg.attrs['units'] == 'mm/day', "Units not properly set in climate_means.py"
    
    # Check converted values (allowing for small floating point differences)
    for day_idx, expected in zip([0, 100, 200, 300], expected_values.values()):
        actual = float(climate_avg.isel(time=day_idx).mean().values)
        np.testing.assert_allclose(
            actual, 
            expected, 
            rtol=1e-2, 
            err_msg=f"Incorrect conversion for {expected} mm/day"
        )

def test_county_stats_conversion():
    """Test the conversion in county_climate_stats.py."""
    from county_climate_stats import compute_daily_zonal_stats, calculate_annual_stats
    
    # Create test data and county boundary
    netcdf_file = create_mock_netcdf()
    county_file = create_mock_county()
    
    # Load the original data
    ds = xr.open_dataset(netcdf_file)
    print("\nOriginal precipitation values:")
    print(f"Light rain: {float(ds.pr[0,0,0].values)}")
    print(f"Moderate rain: {float(ds.pr[100,0,0].values)}")
    print(f"Heavy rain: {float(ds.pr[200,0,0].values)}")
    print(f"Very heavy rain: {float(ds.pr[300,0,0].values)}")
    
    # Convert units manually (simulating what climate_means.py does)
    ds['pr'] = ds['pr'] * 86400
    ds.pr.attrs['units'] = 'mm/day'
    ds.pr.attrs['long_name'] = 'daily precipitation'
    ds.pr.attrs['description'] = 'Daily precipitation converted from kg/m²/s to mm/day'
    
    print("\nAfter conversion to mm/day:")
    print(f"Light rain: {float(ds.pr[0,0,0].values)}")
    print(f"Moderate rain: {float(ds.pr[100,0,0].values)}")
    print(f"Heavy rain: {float(ds.pr[200,0,0].values)}")
    print(f"Very heavy rain: {float(ds.pr[300,0,0].values)}")
    
    counties = gpd.read_parquet(county_file)
    
    # Compute daily statistics
    daily_stats = compute_daily_zonal_stats(ds, counties, 'pr', 'sum', 'output')
    
    print("\nDaily statistics (first few values):")
    print(daily_stats.head())
    
    # Calculate annual statistics for both counties
    total_precip = calculate_annual_stats(daily_stats, 'pr', 'sum')
    heavy_days = calculate_annual_stats(daily_stats, 'pr', 'threshold')
    
    print("\nCalculated statistics:")
    print("Single Cell County:")
    print(f"Total precipitation: {float(total_precip.iloc[0])}")
    print(f"Heavy precipitation days: {float(heavy_days.iloc[0])}")
    print("\nMulti Cell County:")
    print(f"Total precipitation: {float(total_precip.iloc[1])}")
    print(f"Heavy precipitation days: {float(heavy_days.iloc[1])}")
    
    # Expected results
    expected_annual_precip = (
        1.0 * 362 +    # 362 days of light rain (1 mm/day)
        25.0 +         # 1 day of moderate rain (25 mm/day)
        25.4 +         # 1 day of heavy rain (25.4 mm/day)
        50.0           # 1 day of very heavy rain (50 mm/day)
    )
    expected_heavy_days = 2  # Days with > 25.4 mm (very heavy and heavy rain days)
    
    print("\nExpected values:")
    print(f"Expected total precipitation: {expected_annual_precip}")
    print(f"Expected heavy days: {expected_heavy_days}")
    
    # Validate results for both counties
    for i in range(2):
        np.testing.assert_allclose(
            float(total_precip.iloc[i]), 
            expected_annual_precip,
            rtol=1e-2,
            err_msg=f"Incorrect annual precipitation total for county {i+1}"
        )
        
        assert float(heavy_days.iloc[i]) == expected_heavy_days, \
            f"Incorrect count of heavy precipitation days for county {i+1}"

if __name__ == "__main__":
    # Create output directories if they don't exist
    os.makedirs('output/climate_means', exist_ok=True)
    os.makedirs('output/county_climate_stats', exist_ok=True)
    
    # Run tests
    print("Testing climate_means.py precipitation conversions...")
    test_climate_means_conversion()
    print("✓ climate_means.py tests passed")
    
    print("\nTesting county_climate_stats.py precipitation handling...")
    test_county_stats_conversion()
    print("✓ county_climate_stats.py tests passed") 