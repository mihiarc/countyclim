"""
Configuration Management Module

Handles loading, validation, and management of configuration settings
for the climate data processor.
"""

import os
import configparser
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Default configuration values
DEFAULT_CONFIG = {
    'paths': {
        'external_drive_path': '/media/mihiarc/RPA1TB/data/NorESM2-LM',
        'output_dir': 'output/climate_means'
    },
    'processing': {
        'active_scenario': 'historical',
        'parallel_processing': 'True',
        'batch_size': '20',
        'max_workers': '4'
    },
    'variables': {
        'variables': 'tas,tasmax,tasmin,pr'
    },
    'logging': {
        'log_level': 'INFO',
        'log_file': 'climate_processing.log'
    },
    'optimization': {
        'target_chunk_size': '75',
        'enable_compression': 'True',
        'compression_level': '4'
    },
    'validation': {
        'validate_files': 'True',
        'skip_invalid_files': 'True'
    }
}

def create_default_config_file(config_path: str) -> None:
    """Create a default configuration file."""
    config = configparser.ConfigParser()
    
    for section, options in DEFAULT_CONFIG.items():
        config.add_section(section)
        for key, value in options.items():
            config.set(section, key, value)
    
    with open(config_path, 'w') as f:
        config.write(f)
    
    logger.info(f"Created default configuration file: {config_path}")

def load_configuration(config_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from file or create default.
    
    Args:
        config_file: Path to configuration file. If None, uses 'climate_config.ini'
    
    Returns:
        Dictionary containing configuration values
    """
    if config_file is None:
        config_file = 'climate_config.ini'
    
    config_path = Path(config_file)
    
    # Create default config if it doesn't exist
    if not config_path.exists():
        create_default_config_file(str(config_path))
    
    # Load configuration
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # Convert to dictionary with proper types
    config_dict = {}
    
    # Paths
    config_dict['external_drive_path'] = config.get('paths', 'external_drive_path',
                                                   fallback=DEFAULT_CONFIG['paths']['external_drive_path'])
    config_dict['output_dir'] = config.get('paths', 'output_dir',
                                          fallback=DEFAULT_CONFIG['paths']['output_dir'])
    
    # Processing
    config_dict['active_scenario'] = config.get('processing', 'active_scenario',
                                               fallback=DEFAULT_CONFIG['processing']['active_scenario'])
    config_dict['parallel_processing'] = config.getboolean('processing', 'parallel_processing',
                                                          fallback=True)
    config_dict['batch_size'] = config.getint('processing', 'batch_size',
                                             fallback=int(DEFAULT_CONFIG['processing']['batch_size']))
    config_dict['max_workers'] = config.getint('processing', 'max_workers',
                                              fallback=int(DEFAULT_CONFIG['processing']['max_workers']))
    
    # Variables
    variables_str = config.get('variables', 'variables',
                              fallback=DEFAULT_CONFIG['variables']['variables'])
    config_dict['variables'] = [v.strip() for v in variables_str.split(',')]
    
    # Logging
    config_dict['log_level'] = config.get('logging', 'log_level',
                                         fallback=DEFAULT_CONFIG['logging']['log_level'])
    config_dict['log_file'] = config.get('logging', 'log_file',
                                        fallback=DEFAULT_CONFIG['logging']['log_file'])
    
    # Optimization
    config_dict['target_chunk_size'] = config.getint('optimization', 'target_chunk_size',
                                                    fallback=int(DEFAULT_CONFIG['optimization']['target_chunk_size']))
    config_dict['enable_compression'] = config.getboolean('optimization', 'enable_compression',
                                                         fallback=True)
    config_dict['compression_level'] = config.getint('optimization', 'compression_level',
                                                    fallback=int(DEFAULT_CONFIG['optimization']['compression_level']))
    
    # Validation
    config_dict['validate_files'] = config.getboolean('validation', 'validate_files',
                                                     fallback=True)
    config_dict['skip_invalid_files'] = config.getboolean('validation', 'skip_invalid_files',
                                                         fallback=True)
    
    # Store config file path for reference
    config_dict['_config_file'] = str(config_path)
    
    return config_dict

def override_config(config: Dict[str, Any], args) -> Dict[str, Any]:
    """
    Override configuration values with command line arguments.
    
    Args:
        config: Base configuration dictionary
        args: Parsed command line arguments
    
    Returns:
        Updated configuration dictionary
    """
    if args.scenario:
        config['active_scenario'] = args.scenario
        logger.info(f"Scenario overridden to: {args.scenario}")
    
    if args.variables:
        config['variables'] = [v.strip() for v in args.variables.split(',')]
        logger.info(f"Variables overridden to: {config['variables']}")
    
    if args.batch_size:
        config['batch_size'] = args.batch_size
        logger.info(f"Batch size overridden to: {args.batch_size}")
    
    if args.max_workers:
        config['max_workers'] = args.max_workers
        logger.info(f"Max workers overridden to: {args.max_workers}")
    
    return config

def validate_configuration(config: Dict[str, Any]) -> bool:
    """
    Validate configuration values.
    
    Args:
        config: Configuration dictionary
    
    Returns:
        True if configuration is valid, False otherwise
    """
    errors = []
    
    # Validate paths
    if not Path(config['external_drive_path']).exists():
        errors.append(f"External drive path does not exist: {config['external_drive_path']}")
    
    # Validate scenario
    valid_scenarios = ['historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585']
    if config['active_scenario'] not in valid_scenarios:
        errors.append(f"Invalid scenario: {config['active_scenario']}. "
                     f"Valid options: {valid_scenarios}")
    
    # Validate variables
    valid_variables = ['tas', 'tasmax', 'tasmin', 'pr']
    invalid_vars = set(config['variables']) - set(valid_variables)
    if invalid_vars:
        errors.append(f"Invalid variables: {invalid_vars}. "
                     f"Valid options: {valid_variables}")
    
    # Validate numeric values
    if config['batch_size'] <= 0:
        errors.append("Batch size must be positive")
    
    if config['max_workers'] <= 0:
        errors.append("Max workers must be positive")
    
    if config['compression_level'] < 1 or config['compression_level'] > 9:
        errors.append("Compression level must be between 1 and 9")
    
    # Log errors
    for error in errors:
        logger.error(f"Configuration error: {error}")
    
    return len(errors) == 0

def get_config_summary(config: Dict[str, Any]) -> str:
    """
    Generate a summary of the current configuration.
    
    Args:
        config: Configuration dictionary
    
    Returns:
        Formatted configuration summary
    """
    summary = [
        "=== Configuration Summary ===",
        f"External Drive: {config['external_drive_path']}",
        f"Output Directory: {config['output_dir']}",
        f"Active Scenario: {config['active_scenario']}",
        f"Variables: {', '.join(config['variables'])}",
        f"Parallel Processing: {config['parallel_processing']}",
        f"Batch Size: {config['batch_size']}",
        f"Max Workers: {config['max_workers']}",
        f"Log Level: {config['log_level']}",
        f"File Validation: {config['validate_files']}",
        f"Compression: {config['enable_compression']} (level {config['compression_level']})",
        "=========================="
    ]
    
    return "\n".join(summary)