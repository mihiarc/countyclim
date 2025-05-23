#!/usr/bin/env python3
"""
Test script for the refactored climate processing module.

This script performs basic tests to ensure the module is working correctly.
"""

import sys
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from climate_processing import (
    ClimateConfig, 
    RegionManager, 
    PeriodGenerator,
    ClimateProcessor
)


def test_config():
    """Test configuration loading."""
    print("Testing ClimateConfig...")
    
    # Test with defaults
    config = ClimateConfig()
    print(f"  Active scenario: {config.active_scenario}")
    print(f"  Output directory: {config.output_dir}")
    print(f"  Max processes: {config.max_processes}")
    
    # Test with overrides
    config_override = ClimateConfig({
        'active_scenario': 'ssp245',
        'max_processes': 4
    })
    print(f"  Override scenario: {config_override.active_scenario}")
    print(f"  Override processes: {config_override.max_processes}")
    
    print("✓ ClimateConfig tests passed\n")


def test_regions():
    """Test region management."""
    print("Testing RegionManager...")
    
    manager = RegionManager()
    
    # Test listing regions
    regions = manager.list_regions()
    print(f"  Available regions: {regions}")
    
    # Test getting a specific region
    conus = manager.get_region('CONUS')
    print(f"  CONUS region: {conus.name}")
    print(f"  CONUS bounds: lon=[{conus.lon_min}, {conus.lon_max}], "
          f"lat=[{conus.lat_min}, {conus.lat_max}]")
    
    # Test region dictionary conversion
    region_dict = conus.to_dict()
    print(f"  CONUS CRS: EPSG:{region_dict.get('crs_value', 'N/A')}")
    
    print("✓ RegionManager tests passed\n")


def test_periods():
    """Test period generation."""
    print("Testing PeriodGenerator...")
    
    data_availability = {
        'historical': {'start': 1950, 'end': 2014},
        'ssp245': {'start': 2015, 'end': 2100}
    }
    
    generator = PeriodGenerator(data_availability)
    
    # Test historical periods
    hist_periods = generator.generate_periods('historical')
    print(f"  Historical periods: {len(hist_periods)} periods")
    if hist_periods:
        print(f"  First period: {hist_periods[0]}")
        print(f"  Last period: {hist_periods[-1]}")
    
    # Test future scenario periods
    future_periods = generator.generate_periods('ssp245')
    print(f"  SSP245 periods: {len(future_periods)} periods")
    
    print("✓ PeriodGenerator tests passed\n")


def test_processor_init():
    """Test processor initialization."""
    print("Testing ClimateProcessor initialization...")
    
    # Create a test config (validation will fail without actual paths)
    config = ClimateConfig({
        'external_drive_path': '/test/path',
        'base_data_path': '/test/path/data',
        'parallel_processing': False  # Disable for testing
    })
    
    # Create processor
    processor = ClimateProcessor(config)
    print(f"  Processor created successfully")
    print(f"  Region manager initialized: {len(processor.region_manager.list_regions())} regions")
    print(f"  Period generator initialized: {processor.period_generator is not None}")
    
    print("✓ ClimateProcessor initialization tests passed\n")


def main():
    """Run all tests."""
    print("=" * 60)
    print("Climate Processing Module Tests")
    print("=" * 60)
    print()
    
    try:
        test_config()
        test_regions()
        test_periods()
        test_processor_init()
        
        print("=" * 60)
        print("All tests passed! ✓")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError during testing: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main()) 