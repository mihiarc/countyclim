# climate_processor/io_utils.py
"""
I/O Utilities Module

Handles file input/output operations and data saving with optimization.
"""

import logging
import os
from datetime import datetime
from typing import Dict, Any
import xarray as xr

from .regions import REGION_BOUNDS
from .coordinates import convert_longitudes_to_standard

logger = logging.getLogger(__name__)


def save_region_dataset_optimized(region_key: str, dataset: xr.Dataset, scenario: str, 
                                config: Dict[str, Any]) -> str:
    """
    Save region dataset with optimized I/O and proper formatting.
    
    Args:
        region_key: Region identifier
        dataset: Dataset to save
        scenario: Climate scenario
        config: Configuration dictionary
        
    Returns:
        Path to saved file
    """
    region_name = REGION_BOUNDS[region_key]['name'].lower().replace(' ', '_')
    output_dir = config['output_dir']
    
    # Include scenario in output filename with descriptive naming
    if scenario == 'historical':
        output_file = os.path.join(output_dir, f'{region_name}_{scenario}_climate_means.nc')
    else:
        output_file = os.path.join(output_dir, f'{region_name}_{scenario}_climate_means.nc')
    
    logger.info(f"Preparing to save {region_name} data to {output_file}...")
    
    # Convert longitudes if specified in the region settings
    if REGION_BOUNDS[region_key].get('convert_longitudes', False):
        logger.info(f"Converting longitudes for {region_name} from 0-360 to -180 to 180 format...")
        dataset = convert_longitudes_to_standard(dataset)
    
    # Add global attributes to the dataset
    dataset.attrs.update({
        'title': f'Climate Means for {REGION_BOUNDS[region_key]["name"]}',
        'scenario': scenario,
        'methodology': 'Climate normals calculated from daily data',
        'spatial_coverage': REGION_BOUNDS[region_key]['name'],
        'created': datetime.now().isoformat(),
        'created_by': 'Climate Data Processor',
        'description': 'Climate normals calculated using 30-year moving windows',
        'variables': 'pr (precipitation), tas (temperature), tasmax (max temperature), tasmin (min temperature)',
        'units_precipitation': 'mm/day',
        'units_temperature': 'degC',
        'conventions': 'CF-1.8',
        'institution': 'Climate Data Processing Pipeline'
    })
    
    # Optimize encoding for NetCDF output
    encoding = {}
    for var in dataset.data_vars:
        encoding[var] = {
            'zlib': config.get('enable_compression', True),
            'complevel': config.get('compression_level', 4),
            'shuffle': True,       # Enable shuffle filter
            'chunksizes': None,    # Let xarray determine optimal chunks for storage
            'fletcher32': False,   # Disable checksum for speed
        }
        
        # Add appropriate fill values
        if var.startswith('tas'):
            encoding[var]['_FillValue'] = -9999.0
        elif var.startswith('pr'):
            encoding[var]['_FillValue'] = -9999.0
    
    # Save the dataset with optimized settings
    logger.info(f"Computing and saving {region_name} data...")
    
    try:
        dataset.to_netcdf(
            output_file,
            encoding=encoding,
            format='NETCDF4',
            engine='netcdf4'
        )
        
        logger.info(f"✓ Saved {region_name} data to {output_file}")
        logger.info(f"  Variables: {list(dataset.data_vars.keys())}")
        
        # Get the count of variables for each type
        var_counts = {}
        for var_name in dataset.data_vars.keys():
            var_type = var_name.split('_')[0]  # Extract variable type (e.g., 'pr', 'tas')
            var_counts[var_type] = var_counts.get(var_type, 0) + 1
        
        logger.info(f"  Variable counts: {var_counts}")
        
        # Log file size
        file_size = os.path.getsize(output_file) / 1024**2  # MB
        logger.info(f"  File size: {file_size:.1f} MB")
        
        return output_file
        
    except Exception as e:
        logger.error(f"Failed to save {region_name} data: {e}")
        raise


def create_netcdf_encoding(dataset: xr.Dataset, config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Create optimized encoding dictionary for NetCDF output.
    
    Args:
        dataset: Dataset to encode
        config: Configuration dictionary
        
    Returns:
        Encoding dictionary
    """
    encoding = {}
    
    for var in dataset.data_vars:
        var_encoding = {
            'zlib': config.get('enable_compression', True),
            'complevel': config.get('compression_level', 4),
            'shuffle': True,
            'fletcher32': False,
        }
        
        # Variable-specific settings
        if var.startswith('pr'):
            var_encoding['_FillValue'] = -9999.0
            var_encoding['dtype'] = 'float32'  # Precipitation doesn't need double precision
        elif var.startswith('tas'):
            var_encoding['_FillValue'] = -9999.0
            var_encoding['dtype'] = 'float32'  # Temperature doesn't need double precision
        
        # Coordinate encoding
        if 'time' in dataset[var].dims:
            # Use appropriate time encoding
            var_encoding['units'] = 'days since 1850-01-01'
            var_encoding['calendar'] = 'standard'
        
        encoding[var] = var_encoding
    
    # Coordinate variable encoding
    for coord in dataset.coords:
        if coord not in encoding:
            encoding[coord] = {'zlib': False}  # Don't compress coordinate variables
    
    return encoding


def validate_output_file(file_path: str, expected_vars: list = None) -> bool:
    """
    Validate that output file was created correctly.
    
    Args:
        file_path: Path to output file
        expected_vars: List of expected variables
        
    Returns:
        True if file is valid, False otherwise
    """
    try:
        if not os.path.exists(file_path):
            logger.error(f"Output file does not exist: {file_path}")
            return False
        
        # Check file size
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            logger.error(f"Output file is empty: {file_path}")
            return False
        
        # Try to open and validate structure
        with xr.open_dataset(file_path) as ds:
            # Check that we can read the file
            logger.debug(f"Output file structure: {dict(ds.dims)}")
            
            # Check expected variables if provided
            if expected_vars:
                missing_vars = set(expected_vars) - set(ds.data_vars)
                if missing_vars:
                    logger.error(f"Missing variables in output: {missing_vars}")
                    return False
            
            # Check for basic required attributes
            required_attrs = ['title', 'created', 'scenario']
            missing_attrs = [attr for attr in required_attrs if attr not in ds.attrs]
            if missing_attrs:
                logger.warning(f"Missing global attributes: {missing_attrs}")
            
            logger.info(f"✓ Output file validation passed: {file_path}")
            return True
            
    except Exception as e:
        logger.error(f"Output file validation failed for {file_path}: {e}")
        return False


def cleanup_temporary_files(temp_dir: str = None) -> None:
    """
    Clean up temporary files created during processing.
    
    Args:
        temp_dir: Directory containing temporary files
    """
    if temp_dir and os.path.exists(temp_dir):
        try:
            import shutil
            shutil.rmtree(temp_dir)
            logger.info(f"Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            logger.warning(f"Could not clean up temporary directory {temp_dir}: {e}")


def get_output_file_info(file_path: str) -> Dict[str, Any]:
    """
    Get information about an output file.
    
    Args:
        file_path: Path to output file
        
    Returns:
        Dictionary with file information
    """
    info = {
        'path': file_path,
        'exists': False,
        'size_mb': 0,
        'variables': [],
        'dimensions': {},
        'global_attributes': {}
    }
    
    try:
        if os.path.exists(file_path):
            info['exists'] = True
            info['size_mb'] = os.path.getsize(file_path) / 1024**2
            
            with xr.open_dataset(file_path) as ds:
                info['variables'] = list(ds.data_vars.keys())
                info['dimensions'] = dict(ds.dims)
                info['global_attributes'] = dict(ds.attrs)
                
    except Exception as e:
        logger.warning(f"Could not get file info for {file_path}: {e}")
    
    return info