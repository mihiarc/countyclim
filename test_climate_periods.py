#!/usr/bin/env python3
"""
Test script to verify climate period generation for 30-year moving windows.
"""

# Import the necessary functions from climate_means.py
import sys
import os
sys.path.append('.')

# Data availability assumptions (copied from climate_means.py)
DATA_AVAILABILITY = {
    'historical': {'start': 1950, 'end': 2014},
    'ssp126': {'start': 2015, 'end': 2100},
    'ssp245': {'start': 2015, 'end': 2100},
    'ssp370': {'start': 2015, 'end': 2100},
    'ssp585': {'start': 2015, 'end': 2100}
}

def generate_climate_periods(scenario):
    """
    Generate climate periods based on scenario.
    Each period represents a 30-year window for calculating climate normals.
    Each year's climate is based on the preceding 30 years ending in that year.
    """
    periods = []
    
    # Get data availability for this scenario
    if scenario not in DATA_AVAILABILITY:
        print(f"Warning: No data availability info for scenario {scenario}, using historical defaults")
        data_start = 1950
        data_end = 2014
    else:
        data_start = DATA_AVAILABILITY[scenario]['start']
        data_end = DATA_AVAILABILITY[scenario]['end']
    
    if scenario == 'historical':
        # Historical: 1980-2014 (35 years of annual climate measures)
        # Each year's climate is based on the preceding 30 years
        for target_year in range(1980, 2015):
            end_year = target_year      # Climate period ends in the target year
            start_year = target_year - 29  # 30 years total (target_year - 29 to target_year inclusive)
            
            # Ensure we don't go outside available data
            start_year = max(start_year, data_start)
            end_year = min(end_year, data_end)
            
            # Only include if we have at least 20 years of data (2/3 of 30 years)
            if end_year - start_year + 1 >= 20:
                period_name = f"climate_{target_year}"
                periods.append((start_year, end_year, target_year, period_name))
    
    else:
        # Future scenarios: 2015-2100 (86 years of annual climate measures)
        # Each year's climate is based on the preceding 30 years
        for target_year in range(2015, 2101):
            end_year = target_year      # Climate period ends in the target year
            start_year = target_year - 29  # 30 years total (target_year - 29 to target_year inclusive)
            
            # Ensure we don't go outside available data
            start_year = max(start_year, data_start)
            end_year = min(end_year, data_end)
            
            # Only include if we have at least 20 years of data (2/3 of 30 years)
            if end_year - start_year + 1 >= 20:
                period_name = f"climate_{target_year}"
                periods.append((start_year, end_year, target_year, period_name))
    
    return periods

def test_climate_periods():
    """Test climate period generation for all scenarios."""
    
    scenarios = ['historical', 'ssp245', 'ssp585']
    
    for scenario in scenarios:
        print(f"\n{'='*60}")
        print(f"Testing scenario: {scenario}")
        print(f"{'='*60}")
        
        periods = generate_climate_periods(scenario)
        
        print(f"Generated {len(periods)} climate periods")
        
        if periods:
            print(f"First period: {periods[0]}")
            print(f"Last period: {periods[-1]}")
            
            # Show first few and last few periods
            print(f"\nFirst 5 periods:")
            for i, period in enumerate(periods[:5]):
                start_year, end_year, target_year, period_name = period
                window_length = end_year - start_year + 1
                print(f"  {i+1:2d}. Target: {target_year}, Window: {start_year}-{end_year} ({window_length} years)")
            
            if len(periods) > 10:
                print(f"\n... ({len(periods)-10} periods in between) ...\n")
                
                print(f"Last 5 periods:")
                for i, period in enumerate(periods[-5:]):
                    start_year, end_year, target_year, period_name = period
                    window_length = end_year - start_year + 1
                    print(f"  {len(periods)-4+i:2d}. Target: {target_year}, Window: {start_year}-{end_year} ({window_length} years)")
        
        # Check for expected ranges
        if scenario == 'historical':
            expected_targets = list(range(1980, 2015))
            actual_targets = [p[2] for p in periods]
            missing = set(expected_targets) - set(actual_targets)
            if missing:
                print(f"\nWarning: Missing target years: {sorted(missing)}")
            else:
                print(f"\n✓ All expected target years (1980-2014) are present")
                
            # Verify the methodology with specific examples
            print(f"\nMethodology verification:")
            for period in periods[:3]:  # Show first 3 as examples
                start_year, end_year, target_year, period_name = period
                print(f"  Climate for {target_year} based on {start_year}-{end_year} (preceding {end_year-start_year+1} years)")
        
        else:  # projection scenarios
            expected_targets = list(range(2015, 2101))
            actual_targets = [p[2] for p in periods]
            missing = set(expected_targets) - set(actual_targets)
            if missing:
                print(f"\nWarning: Missing target years: {sorted(missing)}")
            else:
                print(f"\n✓ All expected target years (2015-2100) are present")
                
            # Verify the methodology with specific examples
            print(f"\nMethodology verification:")
            for period in periods[:3]:  # Show first 3 as examples
                start_year, end_year, target_year, period_name = period
                print(f"  Climate for {target_year} based on {start_year}-{end_year} (preceding {end_year-start_year+1} years)")

if __name__ == "__main__":
    test_climate_periods() 