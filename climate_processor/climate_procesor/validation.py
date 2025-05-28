
# climate_processor/validation.py
"""
Validation Module

Provides comprehensive validation utilities for NetCDF files and data structures.
"""

import logging
from typing import List, Dict, Any
import xarray as xr
from pathlib import Path

logger = logging.getLogger(__name__)


def validate_netcdf_structure(file_path: str, expected_vars: List[str]) -> bool:
    """
    Validate that NetCDF file has expected variables and dimensions.
    
    Args:
        file_path: Path to NetCDF file
        expected_vars: List of expected variable names
        
    Returns:
        True if file structure is valid
    """
    try:
        with xr.open_dataset(file_path) as ds:
            # Check for expected variables
            missing_vars = set(expected_vars) - set(ds.data_vars)
            if missing_vars:
                logger.warning(f"Missing variables in {Path(file_path).name}: {missing_vars}")
                return False
            
            # Check for required dimensions
            required_dims = ['time', 'lat', 'lon']
            missing_dims = set(required_dims) - set(ds.dims)
            if missing_dims:
                logger.warning(f"Missing dimensions in {Path(file_path).name}: {missing_dims}")
                return False
            
            # Check if time dimension has reasonable size
            if ds.dims.get('time', 0) < 1:
                logger.warning(f"Invalid time dimension size in {Path(file_path).name}")
                return False
            
            # Check coordinate ranges
            if 'lat' in ds.coords:
                lat_min, lat_max = float(ds.lat.min()), float(ds.lat.max())
                if lat_min < -90 or lat_max > 90:
                    logger.warning(f"Invalid latitude range in {Path(file_path).name}: {lat_min} to {lat_max}")
                    return False
            
            if 'lon' in ds.coords:
                lon_min, lon_max = float(ds.lon.min()), float(ds.lon.max())
                if lon_max - lon_min > 361:  # Allow for slight overlap
                    logger.warning(f"Invalid longitude range in {Path(file_path).name}: {lon_min} to {lon_max}")
                    return False
                
            return True
            
    except Exception as e:
        logger.error(f"Cannot read {Path(file_path).name}: {e}")
        return False


def validate_data_quality(data_array: xr.DataArray, variable_name: str) -> Dict[str, Any]:
    """
    Perform data quality checks on a data array.
    
    Args:
        data_array: Input data array
        variable_name: Name of the variable
        
    Returns:
        Dictionary with validation results
    """
    results = {
        'valid': True,
        'warnings': [],
        'errors': [],
        'statistics': {}
    }
    
    try:
        # Check for completely missing data
        if data_array.isnull().all():
            results['valid'] = False
            results['errors'].append("All data is missing (NaN)")
            return results
        
        # Calculate basic statistics
        results['statistics'] = {
            'min': float(data_array.min()),
            'max': float(data_array.max()),
            'mean': float(data_array.mean()),
            'missing_percent': float(data_array.isnull().mean() * 100)
        }
        
        # Check missing data percentage
        missing_percent = results['statistics']['missing_percent']
        if missing_percent > 50:
            results['valid'] = False
            results['errors'].append(f"Too much missing data: {missing_percent:.1f}%")
        elif missing_percent > 10:
            results['warnings'].append(f"High amount of missing data: {missing_percent:.1f}%")
        
        # Variable-specific checks
        min_val = results['statistics']['min']
        max_val = results['statistics']['max']
        
        if variable_name in ['tas', 'tasmax', 'tasmin']:
            # Temperature checks (assuming Kelvin input)
            if min_val < 150 or max_val > 350:  # Extreme but possible values in Kelvin
                results['warnings'].append(f"Temperature values outside typical range: {min_val:.1f} to {max_val:.1f}")
            if min_val < 0:
                results['errors'].append(f"Negative temperature values found: {min_val:.1f}")
                results['valid'] = False
                
        elif variable_name == 'pr':
            # Precipitation checks
            if min_val < 0:
                results['errors'].append(f"Negative precipitation values found: {min_val:.1f}")
                results['valid'] = False
            if max_val > 1:  # kg m^-2 s^-1 (very high but possible)
                results['warnings'].append(f"Very high precipitation values: {max_val:.6f} kg m^-2 s^-1")
        
        # Check for constant values
        if min_val == max_val:
            results['warnings'].append("Data contains only constant values")
        
        # Check for infinite values
        if not data_array.values[~data_array.isnull()].finite().all():
            results['errors'].append("Data contains infinite values")
            results['valid'] = False
        
    except Exception as e:
        results['valid'] = False
        results['errors'].append(f"Error during validation: {e}")
    
    return results