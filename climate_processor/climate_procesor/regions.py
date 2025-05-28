"""
Regional Definitions and Operations

Handles regional boundary definitions, coordinate system conversions,
and regional data extraction operations.
"""

import logging
from typing import Dict, List, Tuple, Any
import xarray as xr
import numpy as np

logger = logging.getLogger(__name__)

# Define regional bounds (for 0-360 longitude system)
REGION_BOUNDS = {
    'CONUS': {
        'name': 'CONUS',
        'lon_min': 234,   # 234°E in 0-360 system (-126°E in -180/180)
        'lon_max': 294,   # 294°E in 0-360 system (-66°E in -180/180)
        'lat_min': 24.0,  # Extended south to fully cover Florida
        'lat_max': 50.0,  # Extended north to ensure coverage
        'convert_longitudes': True
    },
    'AK': {
        'name': 'Alaska',
        'lon_min': 170,   # 170°E in 0-360 system
        'lon_max': 235,   # 235°E in 0-360 system - extended to include SE Alaska
        'lat_min': 50.0,
        'lat_max': 72.0,
        'convert_longitudes': True
    },
    'HI': {
        'name': 'Hawaii and Islands',
        'lon_min': 181.63,   # 181.63 in 0-360 system
        'lon_max': 205.20,   # 205.20 in 0-360 system
        'lat_min': 18.92,
        'lat_max': 28.45,
        'convert_longitudes': True
    },
    'PRVI': {
        'name': 'Puerto Rico and U.S. Virgin Islands',
        'lon_min': 292.03,   # -67.97 in 0-360 system
        'lon_max': 295.49,   # -64.51 in 0-360 system
        'lat_min': 17.62,
        'lat_max': 18.57,
        'convert_longitudes': True
    },
    'GU': {
        'name': 'Guam and Northern Mariana Islands',
        'lon_min': 144.58,   # 144.58°E in 0-360 system
        'lon_max': 146.12,   # 146.12°E in 0-360 system
        'lat_min': 13.18,
        'lat_max': 20.61,
        'convert_longitudes': True
    }
}

def get_region_crs_info(region_key: str) -> Dict[str, Any]:
    """
    Get coordinate reference system information for a specific region.
    
    Args:
        region_key: Region identifier (CONUS, AK, HI, PRVI, GU)
        
    Returns:
        Dictionary with CRS information
    """
    region_crs = {
        'CONUS': {
            'crs_type': 'epsg',
            'crs_value': 5070,  # NAD83 / Conus Albers
            'central_longitude': -96,
            'central_latitude': 37.5,
            'extent': [-125, -65, 25, 50]  # West, East, South, North
        },
        'AK': {
            'crs_type': 'epsg',
            'crs_value': 3338,  # NAD83 / Alaska Albers
            'central_longitude': -154,
            'central_latitude': 50,
            'extent': [-170, -130, 50, 72]
        },
        'HI': {
            'crs_type': 'proj4',
            'crs_value': "+proj=aea +lat_1=8 +lat_2=18 +lat_0=13 +lon_0=157 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs",
            'central_longitude': -157,
            'central_latitude': 20,
            'extent': [-178, -155, 18, 29]
        },
        'PRVI': {
            'crs_type': 'epsg',
            'crs_value': 6566,  # NAD83(2011) / Puerto Rico and Virgin Islands
            'central_longitude': -66,
            'central_latitude': 18,
            'extent': [-68, -64, 17, 19]
        },
        'GU': {
            'crs_type': 'epsg',
            'crs_value': 32655,  # WGS 84 / UTM zone 55N
            'central_longitude': 147,
            'central_latitude': 13.5,
            'extent': [144, 147, 13, 21]
        }
    }
    
    # Return CRS info for the requested region or a default if not found
    if region_key in region_crs:
        return region_crs[region_key]
    else:
        # Default to Albers Equal Area
        return {
            'crs_type': 'epsg',
            'crs_value': 5070,
            'central_longitude': -96,
            'central_latitude': 37.5,
            'extent': [-125, -65, 25, 50]
        }

def convert_longitude_bounds(lon_min: float, lon_max: float, is_0_360: bool) -> Dict[str, float]:
    """
    Convert longitude bounds between 0-360 and -180-180 coordinate systems.
    
    Args:
        lon_min: Minimum longitude in original system
        lon_max: Maximum longitude in original system
        is_0_360: True if data uses 0-360 system
        
    Returns:
        Dictionary with converted longitude bounds
    """
    if is_0_360:
        # Data is in 0-360, use bounds as-is
        return {'lon_min': lon_min, 'lon_max': lon_max}
    else:
        # Convert from 0-360 to -180-180 system
        converted_min = lon_min - 360 if lon_min > 180 else lon_min
        converted_max = lon_max - 360 if lon_max > 180 else lon_max
        return {'lon_min': converted_min, 'lon_max': converted_max}

def extract_region(ds: xr.Dataset, region_bounds: Dict) -> xr.Dataset:
    """
    Extract a specific region from the dataset with improved coordinate handling.
    
    Args:
        ds: Input xarray Dataset
        region_bounds: Dictionary containing regional boundary information
        
    Returns:
        Dataset subset to the specified region
    """
    # Check coordinate names
    lon_name = 'lon' if 'lon' in ds.coords else 'x'
    lat_name = 'lat' if 'lat' in ds.coords else 'y'
    
    # Check longitude range (0-360 or -180-180)
    lon_min = ds[lon_name].min().item()
    lon_max = ds[lon_name].max().item()
    
    # Determine if we're using 0-360 or -180-180 coordinate system
    is_0_360 = lon_min >= 0 and lon_max > 180
    
    # Convert region bounds to match the dataset's coordinate system
    lon_bounds = convert_longitude_bounds(
        region_bounds['lon_min'], 
        region_bounds['lon_max'], 
        is_0_360
    )
    
    # Handle the case where we cross the 0/360 or -180/180 boundary
    if lon_bounds['lon_min'] > lon_bounds['lon_max']:
        region_ds = ds.where(
            ((ds[lon_name] >= lon_bounds['lon_min']) | 
             (ds[lon_name] <= lon_bounds['lon_max'])) & 
            (ds[lat_name] >= region_bounds['lat_min']) & 
            (ds[lat_name] <= region_bounds['lat_max']), 
            drop=True
        )
    else:
        region_ds = ds.where(
            (ds[lon_name] >= lon_bounds['lon_min']) & 
            (ds[lon_name] <= lon_bounds['lon_max']) & 
            (ds[lat_name] >= region_bounds['lat_min']) & 
            (ds[lat_name] <= region_bounds['lat_max']), 
            drop=True
        )
    
    # Check if we have data
    if region_ds[lon_name].size == 0 or region_ds[lat_name].size == 0:
        logger.warning(f"No data found within region bounds after filtering.")
        logger.warning(f"Dataset longitude range: {lon_min} to {lon_max}")
        logger.warning(f"Region bounds: {region_bounds['lon_min']} to {region_bounds['lon_max']} (original)")
        logger.warning(f"Converted bounds: {lon_bounds['lon_min']} to {lon_bounds['lon_max']}")
    
    return region_ds

def generate_climate_periods(scenario: str, data_availability: Dict) -> List[Tuple[int, int, int, str]]:
    """
    Generate climate periods based on scenario.
    For each target year, creates a 30-year period ending in that year.
    
    Args:
        scenario: Climate scenario ('historical' or projection scenario)
        data_availability: Dictionary with data availability info
        
    Returns:
        List of tuples (start_year, end_year, target_year, period_name)
    """
    periods = []
    
    # Get available data range
    data_range = data_availability.get(scenario, {'start': 1850, 'end': 2014})
    data_start = data_range['start']
    data_end = data_range['end']
    
    if scenario == 'historical':
        # For historical, calculate 30-year periods for each year from 1980 to data_end
        for target_year in range(1980, min(2015, data_end + 1)):
            start_year = target_year - 29  # 30-year period ending in target_year
            # Check if we have data for the full period
            if start_year >= data_start:
                period_name = f"historical_{target_year}"
                periods.append((start_year, target_year, target_year, period_name))
            else:
                logger.warning(f"Insufficient data for 30-year period ending in {target_year}")
                logger.warning(f"Would need data from {start_year}, but data starts at {data_start}")
    else:
        # For projections, calculate 30-year periods for each year from 2015 to data_end
        for target_year in range(2015, min(2101, data_end + 1)):
            start_year = target_year - 29  # 30-year period ending in target_year
            period_name = f"{scenario}_{target_year}"
            periods.append((start_year, target_year, target_year, period_name))
    
    if not periods:
        logger.warning(f"No valid periods generated for {scenario}")
    else:
        logger.info(f"Generated {len(periods)} periods for {scenario}:")
        logger.info(f"First period: {periods[0][0]}-{periods[0][1]} (target: {periods[0][2]})")
        logger.info(f"Last period: {periods[-1][0]}-{periods[-1][1]} (target: {periods[-1][2]})")
    
    return periods

def get_region_summary() -> str:
    """
    Generate a summary of all defined regions.
    
    Returns:
        Formatted string describing all regions
    """
    summary = ["=== Defined Regions ==="]
    
    for region_key, region_info in REGION_BOUNDS.items():
        summary.append(f"{region_key}: {region_info['name']}")
        summary.append(f"  Longitude: {region_info['lon_min']} to {region_info['lon_max']}")
        summary.append(f"  Latitude: {region_info['lat_min']} to {region_info['lat_max']}")
        summary.append(f"  Convert coordinates: {region_info['convert_longitudes']}")
        summary.append("")
    
    return "\n".join(summary)

def validate_region_bounds(region_key: str) -> bool:
    """
    Validate that a region key exists and has proper bounds.
    
    Args:
        region_key: Region identifier to validate
        
    Returns:
        True if region is valid, False otherwise
    """
    if region_key not in REGION_BOUNDS:
        logger.error(f"Unknown region key: {region_key}")
        logger.info(f"Available regions: {list(REGION_BOUNDS.keys())}")
        return False
    
    region = REGION_BOUNDS[region_key]
    
    # Check required fields
    required_fields = ['name', 'lon_min', 'lon_max', 'lat_min', 'lat_max']
    missing_fields = [field for field in required_fields if field not in region]
    
    if missing_fields:
        logger.error(f"Region {region_key} missing required fields: {missing_fields}")
        return False
    
    # Check bounds validity
    if region['lon_min'] >= region['lon_max']:
        # Allow crossing dateline for specific cases
        if not (region['lon_min'] > 180 and region['lon_max'] < 180):
            logger.warning(f"Region {region_key} has invalid longitude bounds")
    
    if region['lat_min'] >= region['lat_max']:
        logger.error(f"Region {region_key} has invalid latitude bounds")
        return False
    
    if region['lat_min'] < -90 or region['lat_max'] > 90:
        logger.error(f"Region {region_key} has latitude bounds outside valid range")
        return False
    
    return True