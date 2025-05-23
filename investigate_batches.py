#!/usr/bin/env python3
"""
Investigate batch completion patterns to understand missing counties
"""

import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt

def investigate_batch_completion():
    """Investigate which counties have data and identify patterns."""
    
    # Load the data
    temp_file = "output/county_climate_stats/county_tas_2012-2014_mean_daily.parquet"
    counties_file = "output/county_climate_stats/temp_counties.gpkg"
    
    temp_data = pd.read_parquet(temp_file)
    counties = gpd.read_file(counties_file)
    
    print("=== BATCH COMPLETION INVESTIGATION ===")
    print(f"Total counties in boundaries: {len(counties)}")
    print(f"Counties with temperature data: {len(temp_data.dropna(how='all'))}")
    print(f"Missing counties: {len(counties) - len(temp_data.dropna(how='all'))}")
    
    # Identify which counties have data
    counties_with_data = temp_data.dropna(how='all').index.tolist()
    counties_without_data = temp_data[temp_data.isna().all(axis=1)].index.tolist()
    
    print(f"\nCounty ID ranges:")
    print(f"Counties WITH data: {min(counties_with_data)} to {max(counties_with_data)}")
    print(f"Counties WITHOUT data: {min(counties_without_data)} to {max(counties_without_data)}")
    
    # Check for patterns in missing counties
    print(f"\nPattern analysis:")
    print(f"First 10 counties with data: {sorted(counties_with_data)[:10]}")
    print(f"Last 10 counties with data: {sorted(counties_with_data)[-10:]}")
    print(f"First 10 counties without data: {sorted(counties_without_data)[:10]}")
    print(f"Last 10 counties without data: {sorted(counties_without_data)[-10:]}")
    
    # Check if there's a geographic pattern
    if 'id_numeric' in counties.columns:
        counties_with_data_geo = counties[counties['id_numeric'].isin(counties_with_data)]
        counties_without_data_geo = counties[counties['id_numeric'].isin(counties_without_data)]
        
        print(f"\nGeographic analysis:")
        print(f"Counties with data bounds: {counties_with_data_geo.total_bounds}")
        print(f"Counties without data bounds: {counties_without_data_geo.total_bounds}")
        
        # Check by state
        if 'STUSPS' in counties.columns:
            states_with_data = counties_with_data_geo['STUSPS'].value_counts()
            states_without_data = counties_without_data_geo['STUSPS'].value_counts()
            
            print(f"\nStates with most counties WITH data:")
            print(states_with_data.head(10))
            
            print(f"\nStates with most counties WITHOUT data:")
            print(states_without_data.head(10))
            
            # Check for states with mixed coverage
            all_states = set(states_with_data.index) | set(states_without_data.index)
            mixed_states = []
            for state in all_states:
                with_data = states_with_data.get(state, 0)
                without_data = states_without_data.get(state, 0)
                if with_data > 0 and without_data > 0:
                    mixed_states.append((state, with_data, without_data))
            
            if mixed_states:
                print(f"\nStates with mixed coverage (partial batch completion):")
                for state, with_data, without_data in sorted(mixed_states):
                    total = with_data + without_data
                    coverage = (with_data / total) * 100
                    print(f"  {state}: {with_data}/{total} counties ({coverage:.1f}% complete)")
    
    # Check data completeness for counties that have some data
    counties_with_some_data = temp_data.dropna(how='all')
    if len(counties_with_some_data) > 0:
        print(f"\nData completeness for counties with data:")
        
        # Calculate completeness per county
        completeness_per_county = counties_with_some_data.notna().sum(axis=1) / len(counties_with_some_data.columns)
        
        print(f"Counties with 100% complete data: {(completeness_per_county == 1.0).sum()}")
        print(f"Counties with 75-99% complete data: {((completeness_per_county >= 0.75) & (completeness_per_county < 1.0)).sum()}")
        print(f"Counties with 50-74% complete data: {((completeness_per_county >= 0.50) & (completeness_per_county < 0.75)).sum()}")
        print(f"Counties with <50% complete data: {(completeness_per_county < 0.50).sum()}")
        
        print(f"Mean completeness: {completeness_per_county.mean():.1%}")
        print(f"Min completeness: {completeness_per_county.min():.1%}")
        
        # Check if there's a temporal pattern (some days missing)
        daily_completeness = counties_with_some_data.notna().sum(axis=0) / len(counties_with_some_data)
        incomplete_days = daily_completeness[daily_completeness < 1.0]
        
        if len(incomplete_days) > 0:
            print(f"\nTemporal patterns:")
            print(f"Days with incomplete data: {len(incomplete_days)} out of {len(daily_completeness)}")
            print(f"First incomplete day: {incomplete_days.index[0]}")
            print(f"Last incomplete day: {incomplete_days.index[-1]}")
            print(f"Worst day completeness: {incomplete_days.min():.1%}")

def check_batch_processing_pattern():
    """Check if the missing data follows a batch processing pattern."""
    
    temp_file = "output/county_climate_stats/county_tas_2012-2014_mean_daily.parquet"
    temp_data = pd.read_parquet(temp_file)
    
    print("\n=== BATCH PROCESSING PATTERN ANALYSIS ===")
    
    # Assuming batch size of 30 days, check which batches completed
    batch_size = 30
    total_days = len(temp_data.columns)
    num_batches = (total_days + batch_size - 1) // batch_size
    
    print(f"Expected batches: {num_batches} (batch size: {batch_size})")
    
    counties_with_data = temp_data.dropna(how='all')
    
    for batch_num in range(num_batches):
        start_day = batch_num * batch_size
        end_day = min(start_day + batch_size, total_days)
        batch_cols = temp_data.columns[start_day:end_day]
        
        # Check how many counties have data for this batch
        batch_data = counties_with_data[batch_cols]
        counties_with_batch_data = batch_data.dropna(how='all')
        
        print(f"Batch {batch_num + 1} (days {start_day}-{end_day-1}): {len(counties_with_batch_data)} counties have data")
        
        if len(counties_with_batch_data) != len(counties_with_data):
            print(f"  ⚠️  Incomplete batch! Expected {len(counties_with_data)}, got {len(counties_with_batch_data)}")

if __name__ == "__main__":
    investigate_batch_completion()
    check_batch_processing_pattern() 