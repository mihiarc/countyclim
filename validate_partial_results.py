#!/usr/bin/env python3
"""
Validation script for partial precipitation results
"""

import pandas as pd
import numpy as np
import os

def validate_precipitation_results():
    """Validate the partial precipitation results."""
    
    output_dir = "output/county_climate_stats_partial"
    
    print("=== PARTIAL PRECIPITATION VALIDATION ===")
    
    # Check what files were generated
    if os.path.exists(output_dir):
        files = [f for f in os.listdir(output_dir) if f.endswith('.parquet')]
        print(f"Generated files: {files}")
        
        for file in files:
            file_path = os.path.join(output_dir, file)
            size_mb = os.path.getsize(file_path) / (1024**2)
            print(f"  {file}: {size_mb:.2f} MB")
    
    # Validate precipitation sum (total annual precipitation)
    precip_sum_file = os.path.join(output_dir, "county_pr_2012-2014_sum_daily.parquet")
    if os.path.exists(precip_sum_file):
        print(f"\n=== TOTAL PRECIPITATION VALIDATION ===")
        precip_data = pd.read_parquet(precip_sum_file)
        print(f"Shape: {precip_data.shape}")
        print(f"Counties: {len(precip_data)}")
        print(f"Days: {len(precip_data.columns)}")
        
        # Check for valid data
        valid_data = precip_data.values[~np.isnan(precip_data.values)]
        if len(valid_data) > 0:
            print(f"Valid values: {len(valid_data):,}")
            print(f"Daily precipitation range: {valid_data.min():.2f} to {valid_data.max():.2f} mm")
            print(f"Mean daily precipitation: {valid_data.mean():.2f} mm")
            
            # Convert to inches for US context
            print(f"Daily precipitation range: {valid_data.min()/25.4:.3f} to {valid_data.max()/25.4:.3f} inches")
            print(f"Mean daily precipitation: {valid_data.mean()/25.4:.3f} inches")
            
            # Calculate annual totals for sample counties
            annual_totals = precip_data.sum(axis=1).dropna()
            if len(annual_totals) > 0:
                print(f"\nAnnual precipitation totals:")
                print(f"  Range: {annual_totals.min():.1f} to {annual_totals.max():.1f} mm")
                print(f"  Range: {annual_totals.min()/25.4:.1f} to {annual_totals.max()/25.4:.1f} inches")
                print(f"  Mean: {annual_totals.mean():.1f} mm ({annual_totals.mean()/25.4:.1f} inches)")
                
                # Check if values are reasonable (typical US range: 10-60 inches annually)
                reasonable_min = 10 * 25.4  # 10 inches in mm
                reasonable_max = 60 * 25.4  # 60 inches in mm
                reasonable_count = ((annual_totals >= reasonable_min) & (annual_totals <= reasonable_max)).sum()
                print(f"  Counties with reasonable annual precip (10-60 inches): {reasonable_count}/{len(annual_totals)}")
        else:
            print("⚠️  No valid precipitation data found!")
    
    # Validate precipitation threshold (days above 1 inch)
    precip_threshold_file = os.path.join(output_dir, "county_pr_2012-2014_threshold_daily.parquet")
    if os.path.exists(precip_threshold_file):
        print(f"\n=== HEAVY PRECIPITATION DAYS VALIDATION ===")
        heavy_precip_data = pd.read_parquet(precip_threshold_file)
        print(f"Shape: {heavy_precip_data.shape}")
        
        valid_data = heavy_precip_data.values[~np.isnan(heavy_precip_data.values)]
        if len(valid_data) > 0:
            print(f"Valid values: {len(valid_data):,}")
            print(f"Daily precipitation range: {valid_data.min():.2f} to {valid_data.max():.2f} mm")
            print(f"Mean daily precipitation: {valid_data.mean():.2f} mm")
            
            # Count days above threshold (25.4mm = 1 inch)
            threshold = 25.4
            above_threshold = (valid_data > threshold).sum()
            print(f"Days above 1 inch threshold: {above_threshold:,} out of {len(valid_data):,} ({(above_threshold/len(valid_data))*100:.1f}%)")
            
            # Calculate annual heavy precip days for sample counties
            annual_heavy_days = (heavy_precip_data > threshold).sum(axis=1).dropna()
            if len(annual_heavy_days) > 0:
                print(f"\nAnnual heavy precipitation days (>1 inch):")
                print(f"  Range: {annual_heavy_days.min():.0f} to {annual_heavy_days.max():.0f} days")
                print(f"  Mean: {annual_heavy_days.mean():.1f} days")
                
                # Check if values are reasonable (typical US range: 0-20 days annually)
                reasonable_heavy_days = (annual_heavy_days <= 20).sum()
                print(f"  Counties with reasonable heavy precip days (≤20): {reasonable_heavy_days}/{len(annual_heavy_days)}")
        else:
            print("⚠️  No valid heavy precipitation data found!")
    
    # Check final combined results
    final_csv = os.path.join(output_dir, "county_climate_stats_partial.csv")
    if os.path.exists(final_csv):
        print(f"\n=== FINAL COMBINED RESULTS ===")
        final_data = pd.read_csv(final_csv)
        print(f"Final dataset shape: {final_data.shape}")
        print(f"Columns: {final_data.columns.tolist()}")
        
        # Check precipitation columns
        precip_cols = [col for col in final_data.columns if 'precip' in col.lower()]
        for col in precip_cols:
            valid_values = final_data[col].dropna()
            if len(valid_values) > 0:
                print(f"\n{col}:")
                print(f"  Valid counties: {len(valid_values)}")
                print(f"  Range: {valid_values.min():.2f} to {valid_values.max():.2f}")
                print(f"  Mean: {valid_values.mean():.2f}")
                
                # Sample values
                print(f"  Sample values: {valid_values.head().tolist()}")

def check_data_quality():
    """Check data quality and identify any issues."""
    
    output_dir = "output/county_climate_stats_partial"
    
    print(f"\n=== DATA QUALITY CHECK ===")
    
    # Check both precipitation files
    files_to_check = [
        ("county_pr_2012-2014_sum_daily.parquet", "Total Precipitation"),
        ("county_pr_2012-2014_threshold_daily.parquet", "Heavy Precipitation Days")
    ]
    
    for filename, description in files_to_check:
        filepath = os.path.join(output_dir, filename)
        if os.path.exists(filepath):
            print(f"\n--- {description} ---")
            data = pd.read_parquet(filepath)
            
            # Check for NaN patterns
            total_values = data.size
            nan_values = data.isna().sum().sum()
            valid_values = total_values - nan_values
            
            print(f"Total values: {total_values:,}")
            print(f"Valid values: {valid_values:,} ({(valid_values/total_values)*100:.1f}%)")
            print(f"NaN values: {nan_values:,} ({(nan_values/total_values)*100:.1f}%)")
            
            if valid_values > 0:
                # Check for counties with complete data
                complete_counties = data.dropna(axis=0, how='any')
                partial_counties = data.dropna(axis=0, how='all') 
                
                print(f"Counties with complete data: {len(complete_counties)}")
                print(f"Counties with some data: {len(partial_counties)}")
                
                if len(complete_counties) > 0:
                    print("✅ Some counties have complete data - processing is working!")
                elif len(partial_counties) > 0:
                    print("⚠️  Counties have partial data - may indicate processing issues")
                else:
                    print("❌ No counties have any data - processing failed")

if __name__ == "__main__":
    validate_precipitation_results()
    check_data_quality() 