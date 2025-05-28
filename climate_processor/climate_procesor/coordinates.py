# climate_processor/coordinates.py
"""
Coordinate System Handling Module

Manages coordinate system conversions and spatial operations.
"""

import logging
import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)


def convert_longitudes_to_standard(ds: xr.Dataset) -> xr.Dataset:
    """
    Convert longitudes from 0-360 format to standard -180 to 180 format.
    
    Args:
        ds: Dataset with longitude coordinates in 0-360 format
        
    Returns:
        Dataset with longitude coordinates in -180 to 180 format
    """
    # Make a copy of the dataset to avoid modifying the original
    ds_converted = ds.copy()
    
    # Check if longitude coordinate exists
    if 'lon' not in ds_converted.coords:
        logger.warning("'lon' coordinate not found, skipping longitude conversion")
        return ds_converted
    
    # Convert longitudes that are > 180 to negative values (0-360 → -180 to 180)
    lon_values = ds_converted.lon.values
    new_lon_values = np.where(lon_values > 180, lon_values - 360, lon_values)
    
    # Update the longitude coordinate
    ds_converted = ds_converted.assign_coords(lon=new_lon_values)
    
    # Sort by the new longitudes to maintain increasing order
    ds_converted = ds_converted.sortby('lon')
    
    return ds_converted


def detect_coordinate_system(ds: xr.Dataset) -> str:
    """
    Detect the coordinate system used in the dataset.
    
    Args:
        ds: Input dataset
        
    Returns:
        String indicating coordinate system ('0_360', '-180_180', or 'unknown')
    """
    if 'lon' not in ds.coords:
        return 'unknown'
    
    lon_min = float(ds.lon.min())
    lon_max = float(ds.lon.max())
    
    if lon_min >= 0 and lon_max > 180:
        return '0_360'
    elif lon_min >= -180 and lon_max <= 180:
        return '-180_180'
    else:
        return 'unknown'


def normalize_coordinates(ds: xr.Dataset, target_system: str = '-180_180') -> xr.Dataset:
    """
    Normalize coordinates to a target system.
    
    Args:
        ds: Input dataset
        target_system: Target coordinate system ('0_360' or '-180_180')
        
    Returns:
        Dataset with normalized coordinates
    """
    current_system = detect_coordinate_system(ds)
    
    if current_system == 'unknown':
        logger.warning("Unknown coordinate system, returning dataset unchanged")
        return ds
    
    if current_system == target_system:
        logger.debug(f"Dataset already in {target_system} system")
        return ds
    
    if target_system == '-180_180' and current_system == '0_360':
        return convert_longitudes_to_standard(ds)
    elif target_system == '0_360' and current_system == '-180_180':
        return convert_longitudes_to_0_360(ds)
    else:
        logger.warning(f"Conversion from {current_system} to {target_system} not implemented")
        return ds


def convert_longitudes_to_0_360(ds: xr.Dataset) -> xr.Dataset:
    """
    Convert longitudes from -180 to 180 format to 0-360 format.
    
    Args:
        ds: Dataset with longitude coordinates in -180 to 180 format
        
    Returns:
        Dataset with longitude coordinates in 0-360 format
    """
    ds_converted = ds.copy()
    
    if 'lon' not in ds_converted.coords:
        logger.warning("'lon' coordinate not found, skipping longitude conversion")
        return ds_converted
    
    # Convert negative longitudes to positive (add 360)
    lon_values = ds_converted.lon.values
    new_lon_values = np.where(lon_values < 0, lon_values + 360, lon_values)
    
    # Update the longitude coordinate
    ds_converted = ds_converted.assign_coords(lon=new_lon_values)
    
    # Sort by the new longitudes to maintain increasing order
    ds_converted = ds_converted.sortby('lon')
    
    return ds_converted