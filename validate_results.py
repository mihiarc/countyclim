#!/usr/bin/env python3
"""
Quick validation script to check the county climate statistics results
"""

import pandas as pd
import numpy as np
import os

def validate_results():
    """Validate the generated results and identify issues."""
    
    output_dir = "output/county_climate_stats"
    
    # Check what files exist
    print("=== FILES GENERATED ===")
    if os.path.exists(output_dir):
        files = os.listdir(output_dir)
        for file in sorted(files):
            if file.endswith('.parquet'):
                file_path = os.path.join(output_dir, file)
                size_mb = os.path.getsize(file_path) / (1024**2)
                print(f"{file}: {size_mb:.2f} MB")
    
    # Validate temperature mean data
    temp_file = os.path.join(output_dir, "county_tas_2012-2014_mean_daily.parquet")
    if os.path.exists(temp_file):
        print("\n=== TEMPERATURE MEAN VALIDATION ===")
        temp_mean = pd.read_parquet(temp_file)
        print(f"Shape: {temp_mean.shape}")
        print(f"Date range: {temp_mean.columns.min()} to {temp_mean.columns.max()}")
        
        # Focus on CONUS counties (those with valid data)
        conus_counties = temp_mean.dropna(how='all')
        non_conus_counties = temp_mean[temp_mean.isna().all(axis=1)]
        
        print(f"\n--- CONUS vs Non-CONUS Breakdown ---")
        print(f"Total counties: {len(temp_mean)}")
        print(f"CONUS counties (with data): {len(conus_counties)}")
        print(f"Non-CONUS counties (no data): {len(non_conus_counties)}")
        print(f"CONUS coverage: {(len(conus_counties)/len(temp_mean))*100:.1f}%")
        
        if len(conus_counties) > 0:
            # Analyze CONUS data quality
            conus_values = conus_counties.values[~np.isnan(conus_counties.values)]
            total_conus_values = conus_counties.size
            valid_conus_values = len(conus_values)
            
            print(f"\n--- CONUS Data Quality ---")
            print(f"Total CONUS values: {total_conus_values:,}")
            print(f"Valid CONUS values: {valid_conus_values:,}")
            print(f"CONUS completeness: {(valid_conus_values/total_conus_values)*100:.1f}%")
            print(f"Temperature range: {conus_values.min():.2f}°C to {conus_values.max():.2f}°C")
            print(f"Temperature range: {conus_values.min()*9/5+32:.1f}°F to {conus_values.max()*9/5+32:.1f}°F")
            print(f"Mean temperature: {conus_values.mean():.2f}°C ({conus_values.mean()*9/5+32:.1f}°F)")
            print(f"Temperature std: {conus_values.std():.2f}°C")
            
            # Sample some counties with complete data
            complete_counties = conus_counties.dropna(axis=0, how='any')
            if len(complete_counties) > 0:
                print(f"Counties with complete data: {len(complete_counties)}")
                sample_county = complete_counties.iloc[0]
                annual_mean = sample_county.mean()
                print(f"Sample county annual mean: {annual_mean:.2f}°C ({annual_mean*9/5+32:.1f}°F)")
    
    # Validate hot days data
    hot_file = os.path.join(output_dir, "county_tasmax_2012-2014_threshold_daily.parquet")
    if os.path.exists(hot_file):
        print("\n=== HOT DAYS VALIDATION ===")
        hot_days = pd.read_parquet(hot_file)
        
        # Focus on CONUS counties
        conus_hot = hot_days.dropna(how='all')
        
        if len(conus_hot) > 0:
            conus_hot_values = conus_hot.values[~np.isnan(conus_hot.values)]
            print(f"CONUS counties with hot day data: {len(conus_hot)}")
            print(f"Hot day temperature range: {conus_hot_values.min():.2f}°C to {conus_hot_values.max():.2f}°C")
            print(f"Hot day temperature range: {conus_hot_values.min()*9/5+32:.1f}°F to {conus_hot_values.max()*9/5+32:.1f}°F")
            print(f"Mean hot day temperature: {conus_hot_values.mean():.2f}°C ({conus_hot_values.mean()*9/5+32:.1f}°F)")
            
            # Check threshold logic (should be counting days > 32.22°C = 90°F)
            above_threshold = (conus_hot_values > 32.22).sum()
            total_hot_values = len(conus_hot_values)
            print(f"Values above 90°F threshold: {above_threshold:,} out of {total_hot_values:,} ({(above_threshold/total_hot_values)*100:.1f}%)")
    
    # Check county boundaries for context
    counties_file = os.path.join(output_dir, "temp_counties.gpkg")
    if os.path.exists(counties_file):
        print(f"\n=== COUNTY BOUNDARIES CONTEXT ===")
        import geopandas as gpd
        counties = gpd.read_file(counties_file)
        print(f"Total counties: {len(counties)}")
        print(f"CRS: {counties.crs}")
        print(f"Full bounds: {counties.total_bounds}")
        
        # Try to identify CONUS vs non-CONUS
        if 'STUSPS' in counties.columns:
            # Non-CONUS state codes
            non_conus_states = ['AK', 'HI', 'PR', 'VI', 'GU', 'MP', 'AS']
            conus_counties_geo = counties[~counties['STUSPS'].isin(non_conus_states)]
            non_conus_counties_geo = counties[counties['STUSPS'].isin(non_conus_states)]
            
            print(f"CONUS counties (geographic): {len(conus_counties_geo)}")
            print(f"Non-CONUS counties (geographic): {len(non_conus_counties_geo)}")
            print(f"CONUS bounds: {conus_counties_geo.total_bounds}")
            
            if len(non_conus_counties_geo) > 0:
                print(f"Non-CONUS states: {sorted(non_conus_counties_geo['STUSPS'].unique())}")

def validate_annual_stats():
    """Validate annual statistics if they exist."""
    output_dir = "output/county_climate_stats"
    
    # Check for final results
    csv_file = os.path.join(output_dir, "county_climate_stats.csv")
    if os.path.exists(csv_file):
        print("\n=== ANNUAL STATISTICS VALIDATION ===")
        annual_stats = pd.read_csv(csv_file)
        print(f"Annual stats shape: {annual_stats.shape}")
        print(f"Columns: {annual_stats.columns.tolist()}")
        
        # Check each climate variable
        climate_cols = [col for col in annual_stats.columns 
                       if col in ['mean_annual_temp', 'days_above_90F', 'days_below_32F', 
                                 'total_annual_precip', 'days_above_1inch_precip']]
        
        for col in climate_cols:
            if col in annual_stats.columns:
                valid_data = annual_stats[col].dropna()
                print(f"\n{col}:")
                print(f"  Valid counties: {len(valid_data)}")
                print(f"  Range: {valid_data.min():.2f} to {valid_data.max():.2f}")
                print(f"  Mean: {valid_data.mean():.2f}")

if __name__ == "__main__":
    validate_results()
    validate_annual_stats() 