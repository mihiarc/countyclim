#!/usr/bin/env python3
"""
Quick test script to verify precipitation processing works with just 5 counties
"""

import os
import sys

# Add the current directory to the path so we can import from county_stats_partial
sys.path.insert(0, '.')

# Import the partial script functions
from county_stats_partial import (
    load_climate_data, 
    load_county_boundaries_subset, 
    compute_daily_zonal_stats_fast,
    calculate_annual_stats
)

import logging
import time
import dask
from dask.distributed import Client, LocalCluster

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def quick_test():
    """Test precipitation processing with just 5 counties."""
    
    start_time = time.time()
    
    # Define paths
    climate_data_path = "output/climate_means/conus_historical_climate_averages.nc"
    counties_path = "output/county_boundaries/contiguous_us_counties.parquet"
    output_dir = "output/test_precip_quick"
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    logger.info("=== QUICK PRECIPITATION TEST (5 COUNTIES) ===")
    
    # Initialize Dask client with minimal resources
    logger.info("Initializing Dask client")
    cluster = LocalCluster(n_workers=2, memory_limit='4GB')
    client = Client(cluster)
    logger.info(f"Dask dashboard: {client.dashboard_link}")
    
    try:
        # Load data - just 5 counties
        climate_data = load_climate_data(climate_data_path)
        counties = load_county_boundaries_subset(counties_path, subset_size=5)
        
        logger.info(f"Testing with {len(counties)} counties:")
        for i, row in counties.iterrows():
            logger.info(f"  {row.get('NAME', 'Unknown')}, {row.get('STUSPS', 'Unknown')}")
        
        # Test precipitation sum
        logger.info("\n--- Testing precipitation sum ---")
        daily_precip = compute_daily_zonal_stats_fast(
            climate_data, counties, 'pr_2012-2014', 'sum', output_dir
        )
        
        logger.info(f"Daily precipitation shape: {daily_precip.shape}")
        logger.info(f"Sample values: {daily_precip.iloc[0, :5].values}")
        
        # Calculate annual total
        annual_precip = calculate_annual_stats(daily_precip, 'pr_2012-2014', 'sum')
        logger.info(f"Annual precipitation range: {annual_precip.min():.1f} to {annual_precip.max():.1f} mm")
        logger.info(f"Annual precipitation (inches): {annual_precip.min()/25.4:.1f} to {annual_precip.max()/25.4:.1f}")
        
        # Test precipitation threshold
        logger.info("\n--- Testing precipitation threshold ---")
        daily_heavy = compute_daily_zonal_stats_fast(
            climate_data, counties, 'pr_2012-2014', 'threshold', output_dir
        )
        
        logger.info(f"Daily heavy precip shape: {daily_heavy.shape}")
        logger.info(f"Sample values: {daily_heavy.iloc[0, :5].values}")
        
        # Calculate annual heavy days
        annual_heavy = calculate_annual_stats(daily_heavy, 'pr_2012-2014', 'threshold')
        logger.info(f"Annual heavy precip days range: {annual_heavy.min():.0f} to {annual_heavy.max():.0f}")
        
        # Save test results
        test_results = counties[['id_numeric', 'GEOID', 'NAME', 'STUSPS']].copy()
        test_results['total_annual_precip_mm'] = annual_precip
        test_results['total_annual_precip_inches'] = annual_precip / 25.4
        test_results['days_above_1inch_precip'] = annual_heavy
        
        output_file = os.path.join(output_dir, "quick_test_results.csv")
        test_results.to_csv(output_file, index=False)
        logger.info(f"Saved test results to {output_file}")
        
        # Print results
        logger.info("\n=== TEST RESULTS ===")
        for _, row in test_results.iterrows():
            logger.info(f"{row['NAME']}, {row['STUSPS']}:")
            logger.info(f"  Annual precip: {row['total_annual_precip_inches']:.1f} inches")
            logger.info(f"  Heavy precip days: {row['days_above_1inch_precip']:.0f}")
        
        total_time = time.time() - start_time
        logger.info(f"\nQuick test completed in {total_time:.1f} seconds")
        
        # Check if results look reasonable
        if (test_results['total_annual_precip_inches'] > 5).all() and \
           (test_results['total_annual_precip_inches'] < 100).all():
            logger.info("✅ Precipitation values look reasonable!")
        else:
            logger.warning("⚠️  Some precipitation values may be unrealistic")
            
        if (test_results['days_above_1inch_precip'] >= 0).all() and \
           (test_results['days_above_1inch_precip'] <= 50).all():
            logger.info("✅ Heavy precipitation days look reasonable!")
        else:
            logger.warning("⚠️  Some heavy precipitation day counts may be unrealistic")
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
        
    finally:
        client.close()
        cluster.close()

if __name__ == "__main__":
    success = quick_test()
    if success:
        print("\n🎉 Quick test passed! Ready to run the full partial script.")
    else:
        print("\n❌ Quick test failed. Check the logs above.")
        sys.exit(1) 