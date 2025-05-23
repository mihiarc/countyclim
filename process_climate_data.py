#!/usr/bin/env python3
"""
Main script for processing climate data using the climate_processing module.

This script demonstrates how to use the refactored climate processing pipeline.
"""

import argparse
import sys
from pathlib import Path

# Add the current directory to Python path to import local module
sys.path.insert(0, str(Path(__file__).parent))

from climate_processing import ClimateConfig, ClimateProcessor


def main():
    """Main entry point for the climate data processing script."""
    
    # Set up command line arguments
    parser = argparse.ArgumentParser(
        description='Process climate data to generate 30-year climate normals'
    )
    parser.add_argument(
        '--scenario', 
        type=str, 
        default='historical',
        choices=['historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585'],
        help='Climate scenario to process (default: historical)'
    )
    parser.add_argument(
        '--external-drive',
        type=str,
        help='Path to external drive (overrides CLIMATE_EXTERNAL_DRIVE env var)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='output/climate_means',
        help='Output directory for processed data (default: output/climate_means)'
    )
    parser.add_argument(
        '--no-parallel',
        action='store_true',
        help='Disable parallel processing'
    )
    parser.add_argument(
        '--max-processes',
        type=int,
        help='Maximum number of processes for parallel processing'
    )
    parser.add_argument(
        '--memory-limit',
        type=str,
        help='Memory limit per worker (e.g., "8GB")'
    )
    
    args = parser.parse_args()
    
    # Create configuration
    config_overrides = {
        'active_scenario': args.scenario,
        'output_dir': args.output_dir,
        'parallel_processing': not args.no_parallel
    }
    
    # Add optional overrides
    if args.external_drive:
        config_overrides['external_drive_path'] = args.external_drive
        config_overrides['base_data_path'] = f'{args.external_drive}/data/NorESM2-LM'
    
    if args.max_processes:
        config_overrides['max_processes'] = args.max_processes
    
    if args.memory_limit:
        config_overrides['memory_limit'] = args.memory_limit
    
    # Create configuration and processor
    try:
        config = ClimateConfig(config_overrides)
        processor = ClimateProcessor(config)
        
        # Run processing
        processor.process()
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main()) 