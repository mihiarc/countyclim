#!/usr/bin/env python3
"""
Climate Data Processor - Main Entry Point

A modular, efficient climate data processing pipeline for calculating
30-year climate normals from daily NetCDF files.
"""

import argparse
import sys
from pathlib import Path
import logging

# Add the package to the Python path
sys.path.insert(0, str(Path(__file__).parent))

from climate_procesor.config import load_configuration, override_config
from climate_procesor.data_processor import ClimateDataProcessor
from utils.diagnostics import run_system_diagnostics
from utils.logging_utils import setup_logging

logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Enhanced Climate Data Processor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Run with default config
  %(prog)s --diagnostics                     # Run system diagnostics
  %(prog)s --scenario ssp245                 # Override scenario
  %(prog)s --variables "tas,pr"              # Process specific variables
  %(prog)s --config /path/to/config.ini     # Use custom config file
        """
    )
    
    parser.add_argument('--diagnostics', action='store_true',
                       help='Run system diagnostics only')
    parser.add_argument('--config', type=str,
                       help='Path to configuration file')
    parser.add_argument('--scenario', type=str,
                       help='Override active scenario')
    parser.add_argument('--variables', type=str,
                       help='Comma-separated list of variables to process')
    parser.add_argument('--batch-size', type=int,
                       help='Override batch size for processing')
    parser.add_argument('--max-workers', type=int,
                       help='Override maximum number of workers')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    return parser.parse_args()


def main():
    """Main entry point for the climate data processor."""
    args = parse_arguments()
    
    try:
        # Load configuration
        config = load_configuration(args.config)
        
        # Override configuration with command line arguments
        config = override_config(config, args)
        
        # Set up logging
        setup_logging(config, verbose=args.verbose)
        
        logger.info("=== Climate Data Processor Starting ===")
        logger.info(f"Configuration loaded from: {config.get('_config_file', 'default')}")
        
        # Run diagnostics if requested
        if args.diagnostics:
            logger.info("Running system diagnostics...")
            run_system_diagnostics(config)
            return 0
        
        # Initialize and run the processor
        processor = ClimateDataProcessor(config)
        
        success = processor.run()
        
        if success:
            logger.info("=== Processing completed successfully ===")
            return 0
        else:
            logger.error("=== Processing failed ===")
            return 1
            
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())