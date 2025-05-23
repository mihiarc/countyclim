#!/usr/bin/env python3
"""
Runner script for precipitation validation tests
"""

import subprocess
import sys
import os
import time

def run_command(command, description):
    """Run a command and return success status."""
    print(f"\n{'='*60}")
    print(f"🚀 {description}")
    print(f"{'='*60}")
    print(f"Command: {command}")
    print()
    
    start_time = time.time()
    
    try:
        result = subprocess.run(command, shell=True, check=True, 
                              capture_output=False, text=True)
        duration = time.time() - start_time
        print(f"\n✅ {description} completed successfully in {duration:.1f} seconds")
        return True
    except subprocess.CalledProcessError as e:
        duration = time.time() - start_time
        print(f"\n❌ {description} failed after {duration:.1f} seconds")
        print(f"Exit code: {e.returncode}")
        return False

def main():
    """Run the precipitation validation workflow."""
    
    print("🌧️  PRECIPITATION VALIDATION WORKFLOW")
    print("This will run a series of tests to validate precipitation processing")
    
    # Check if required files exist
    required_files = [
        "output/climate_means/conus_historical_climate_averages.nc",
        "output/county_boundaries/contiguous_us_counties.parquet"
    ]
    
    missing_files = [f for f in required_files if not os.path.exists(f)]
    if missing_files:
        print(f"\n❌ Missing required files:")
        for f in missing_files:
            print(f"  - {f}")
        print("\nPlease run the data preparation scripts first.")
        sys.exit(1)
    
    print(f"\n✅ All required files found")
    
    # Step 1: Quick test with 5 counties
    success = run_command(
        "python test_precipitation_quick.py",
        "Quick Test (5 counties)"
    )
    
    if not success:
        print("\n❌ Quick test failed. Stopping workflow.")
        sys.exit(1)
    
    # Step 2: Partial run with 100 counties
    print(f"\n⏳ Quick test passed! Proceeding to partial run...")
    time.sleep(2)
    
    success = run_command(
        "python county_stats_partial.py",
        "Partial Run (100 counties)"
    )
    
    if not success:
        print("\n❌ Partial run failed. Stopping workflow.")
        sys.exit(1)
    
    # Step 3: Validate results
    print(f"\n⏳ Partial run completed! Validating results...")
    time.sleep(2)
    
    success = run_command(
        "python validate_partial_results.py",
        "Results Validation"
    )
    
    if success:
        print(f"\n🎉 ALL TESTS PASSED!")
        print(f"Precipitation processing is working correctly.")
        print(f"\nNext steps:")
        print(f"  1. Review the validation output above")
        print(f"  2. Check files in output/county_climate_stats_partial/")
        print(f"  3. If satisfied, run the full county_stats.py script")
    else:
        print(f"\n⚠️  Validation had issues. Check the output above.")
        print(f"The partial run may have completed but results need review.")

if __name__ == "__main__":
    main() 