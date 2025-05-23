#!/usr/bin/env python3
"""
Debug script to examine precipitation values and understand the threshold issue
"""

import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def examine_precipitation_data():
    """Examine the precipitation data to understand the threshold issue."""
    
    print("=== PRECIPITATION DATA EXAMINATION ===")
    
    # Load climate data
    climate_data_path = "output/climate_means/conus_historical_climate_averages.nc"
    print(f"Loading data from: {climate_data_path}")
    
    ds = xr.open_dataset(climate_data_path)
    
    # Examine precipitation variable
    pr_var = 'pr_2012-2014'
    if pr_var in ds.data_vars:
        pr_data = ds[pr_var]
        print(f"\nPrecipitation variable: {pr_var}")
        print(f"Shape: {pr_data.shape}")
        print(f"Dimensions: {pr_data.dims}")
        
        # Get some sample data
        sample_data = pr_data.isel(time=slice(0, 10)).compute()
        print(f"\nSample data shape: {sample_data.shape}")
        
        # Check data range
        print(f"\nData statistics:")
        print(f"  Min: {sample_data.min().values:.6f}")
        print(f"  Max: {sample_data.max().values:.6f}")
        print(f"  Mean: {sample_data.mean().values:.6f}")
        print(f"  Std: {sample_data.std().values:.6f}")
        
        # Check for non-zero values
        non_zero = sample_data.values[sample_data.values > 0]
        print(f"\nNon-zero values:")
        print(f"  Count: {len(non_zero)}")
        if len(non_zero) > 0:
            print(f"  Min non-zero: {non_zero.min():.6f}")
            print(f"  Max non-zero: {non_zero.max():.6f}")
            print(f"  Mean non-zero: {non_zero.mean():.6f}")
        
        # Check units and attributes
        print(f"\nVariable attributes:")
        for attr, value in pr_data.attrs.items():
            print(f"  {attr}: {value}")
        
        # Check threshold
        threshold_mm = 25.4  # 1 inch in mm
        print(f"\nThreshold analysis (1 inch = {threshold_mm} mm):")
        
        # Count values above threshold
        above_threshold = (sample_data > threshold_mm).sum().values
        total_values = sample_data.size
        print(f"  Values above threshold: {above_threshold} / {total_values}")
        print(f"  Percentage above threshold: {(above_threshold/total_values)*100:.2f}%")
        
        # Check if values might be in different units
        print(f"\nUnit conversion checks:")
        
        # Maybe it's in meters instead of mm?
        threshold_m = 0.0254  # 1 inch in meters
        above_threshold_m = (sample_data > threshold_m).sum().values
        print(f"  If data is in meters (threshold = {threshold_m} m): {above_threshold_m} values above")
        
        # Maybe it's in kg/m²/s (flux units)?
        # 1 mm/day = 1 kg/m²/day = 1/(24*3600) kg/m²/s ≈ 1.157e-5 kg/m²/s
        threshold_flux = 25.4 / (24 * 3600)  # 1 inch/day in kg/m²/s
        above_threshold_flux = (sample_data > threshold_flux).sum().values
        print(f"  If data is in kg/m²/s (threshold = {threshold_flux:.2e} kg/m²/s): {above_threshold_flux} values above")
        
        # Check some specific grid points
        print(f"\nSample values from different locations:")
        ny, nx = sample_data.shape[1], sample_data.shape[2]
        locations = [
            (ny//4, nx//4, "Northwest"),
            (ny//4, 3*nx//4, "Northeast"), 
            (3*ny//4, nx//4, "Southwest"),
            (3*ny//4, 3*nx//4, "Southeast"),
            (ny//2, nx//2, "Center")
        ]
        
        for y, x, name in locations:
            values = sample_data[:, y, x].values
            non_zero_vals = values[values > 0]
            print(f"  {name} ({y}, {x}):")
            print(f"    Range: {values.min():.6f} to {values.max():.6f}")
            if len(non_zero_vals) > 0:
                print(f"    Non-zero range: {non_zero_vals.min():.6f} to {non_zero_vals.max():.6f}")
                print(f"    Sample non-zero values: {non_zero_vals[:5]}")
        
        # Check time dimension
        print(f"\nTime information:")
        print(f"  Time range: {pr_data.time.values[0]} to {pr_data.time.values[-1]}")
        print(f"  Number of time steps: {len(pr_data.time)}")
        
        # Check if this is daily data
        if len(pr_data.time) > 1:
            time_diff = pd.to_datetime(pr_data.time.values[1]) - pd.to_datetime(pr_data.time.values[0])
            print(f"  Time step: {time_diff}")
        
    else:
        print(f"❌ Precipitation variable '{pr_var}' not found!")
        print(f"Available variables: {list(ds.data_vars.keys())}")

def check_realistic_precipitation():
    """Check if precipitation values are realistic for US climate."""
    
    print(f"\n=== REALISTIC PRECIPITATION CHECK ===")
    
    # Load a small sample and convert to common units
    ds = xr.open_dataset("output/climate_means/conus_historical_climate_averages.nc")
    pr_data = ds['pr_2012-2014'].isel(time=slice(0, 365)).compute()  # One year
    
    print(f"Analyzing one year of data...")
    
    # Calculate annual totals for each grid point
    annual_totals = pr_data.sum(dim='time')
    
    print(f"Annual precipitation totals (in original units):")
    print(f"  Min: {annual_totals.min().values:.6f}")
    print(f"  Max: {annual_totals.max().values:.6f}")
    print(f"  Mean: {annual_totals.mean().values:.6f}")
    
    # Convert to different units and see what makes sense
    print(f"\nUnit interpretations:")
    
    # If original units are mm/day
    print(f"  If mm/day -> annual mm: {annual_totals.min().values:.1f} to {annual_totals.max().values:.1f}")
    print(f"  If mm/day -> annual inches: {annual_totals.min().values/25.4:.1f} to {annual_totals.max().values/25.4:.1f}")
    
    # If original units are m/day  
    print(f"  If m/day -> annual mm: {annual_totals.min().values*1000:.1f} to {annual_totals.max().values*1000:.1f}")
    print(f"  If m/day -> annual inches: {annual_totals.min().values*1000/25.4:.1f} to {annual_totals.max().values*1000/25.4:.1f}")
    
    # If original units are kg/m²/s (need to convert to daily first)
    daily_from_flux = annual_totals * 24 * 3600  # Convert flux to daily
    print(f"  If kg/m²/s -> annual mm: {daily_from_flux.min().values:.1f} to {daily_from_flux.max().values:.1f}")
    print(f"  If kg/m²/s -> annual inches: {daily_from_flux.min().values/25.4:.1f} to {daily_from_flux.max().values/25.4:.1f}")
    
    print(f"\nTypical US annual precipitation: 10-60 inches (254-1524 mm)")
    print(f"Which interpretation looks most realistic?")

def analyze_appropriate_thresholds():
    """Analyze what precipitation thresholds would be appropriate for this dataset."""
    
    print(f"\n=== THRESHOLD ANALYSIS ===")
    
    # Load precipitation data
    ds = xr.open_dataset("output/climate_means/conus_historical_climate_averages.nc")
    pr_data = ds['pr_2012-2014'].isel(time=slice(0, 365)).compute()  # One year
    
    # Get all non-zero precipitation values
    all_values = pr_data.values.flatten()
    non_zero_values = all_values[all_values > 0]
    
    print(f"Daily precipitation statistics (mm/day):")
    print(f"  Count of non-zero days: {len(non_zero_values):,}")
    print(f"  Min: {non_zero_values.min():.3f}")
    print(f"  Max: {non_zero_values.max():.3f}")
    print(f"  Mean: {non_zero_values.mean():.3f}")
    print(f"  Median: {np.median(non_zero_values):.3f}")
    
    # Calculate percentiles
    percentiles = [50, 75, 90, 95, 99, 99.5, 99.9]
    print(f"\nPercentiles of daily precipitation:")
    for p in percentiles:
        value = np.percentile(non_zero_values, p)
        print(f"  {p:4.1f}th percentile: {value:6.3f} mm/day ({value/25.4:.3f} inches/day)")
    
    # Test different thresholds
    print(f"\nThreshold analysis:")
    test_thresholds_mm = [5, 10, 15, 20, 25, 25.4]  # mm/day
    
    for threshold in test_thresholds_mm:
        above_threshold = (non_zero_values > threshold).sum()
        percentage = (above_threshold / len(non_zero_values)) * 100
        print(f"  {threshold:4.1f} mm/day ({threshold/25.4:.2f} inches): {above_threshold:,} days ({percentage:.2f}%)")
    
    # Suggest appropriate thresholds
    print(f"\nSuggested thresholds for 'heavy precipitation':")
    
    # 95th percentile (heavy precipitation)
    p95 = np.percentile(non_zero_values, 95)
    print(f"  95th percentile: {p95:.1f} mm/day ({p95/25.4:.2f} inches/day)")
    
    # 99th percentile (very heavy precipitation)  
    p99 = np.percentile(non_zero_values, 99)
    print(f"  99th percentile: {p99:.1f} mm/day ({p99/25.4:.2f} inches/day)")
    
    # Common meteorological thresholds
    print(f"\nCommon meteorological thresholds:")
    print(f"  Light rain: 0.1-2.5 mm/day")
    print(f"  Moderate rain: 2.5-10 mm/day") 
    print(f"  Heavy rain: 10-50 mm/day")
    print(f"  Very heavy rain: >50 mm/day")
    
    # For this dataset, suggest 95th percentile
    suggested_threshold = np.percentile(non_zero_values, 95)
    print(f"\n💡 RECOMMENDATION:")
    print(f"   Use {suggested_threshold:.1f} mm/day ({suggested_threshold/25.4:.2f} inches/day) as 'heavy precipitation' threshold")
    print(f"   This represents the 95th percentile of precipitation days")

if __name__ == "__main__":
    examine_precipitation_data()
    check_realistic_precipitation()
    analyze_appropriate_thresholds() 