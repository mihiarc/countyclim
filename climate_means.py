#!/usr/bin/env python3
"""
Climate Data Processing Script - Enhanced and Fixed Version

Processes daily temperature and precipitation NetCDF files to calculate
30-year climate normals for US territories with optimized Dask configuration.

Data Requirements:
----------------
This script is designed to work with 30-year periods of daily climate data.
For historical analysis (1981-2010 climate normal):
- Daily data files from 1981-2010 named as: {var}_day_NorESM2-LM_historical_r1i1p1f1_gn_{year}_v1.1.nc
  where {var} is one of: tas, tasmax, tasmin, pr
  and {year} ranges from 1981 to 2010

For future projections (2071-2100 climate normal):
- Daily data files from 2071-2100 for each SSP scenario
- File naming: {var}_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}_v1.1.nc
  where scenario is one of: ssp126, ssp245, ssp370, ssp585

Current Limitations:
-----------------
The current data directory only contains years 2012-2014.
To properly calculate 30-year climate normals, please obtain the complete dataset.
"""

import os
import xarray as xr
import numpy as np
import glob
from datetime import datetime
import multiprocessing as mp
import dask
from dask.distributed import Client, LocalCluster
from dask.diagnostics import ProgressBar
import psutil
import platform
import logging
import configparser
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from tqdm import tqdm
import gc

# Import the file handler module (assuming it's in the same directory)
from file_path_handler import create_file_handler, ClimateFileHandler

# Set up logging with better formatting
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('climate_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration Management
def load_configuration():
    """Load configuration from file or use defaults."""
    config = configparser.ConfigParser()
    
    # Create default config if it doesn't exist
    config_file = 'climate_config.ini'
    if not os.path.exists(config_file):
        config['paths'] = {
            'external_drive_path': '/media/mihiarc/RPA1TB/data/NorESM2-LM',
            'output_dir': 'output/climate_means'
        }
        config['processing'] = {
            'active_scenario': 'historical',
            'parallel_processing': 'True',
            'batch_size': '20',
            'max_workers': '4'
        }
        config['variables'] = {
            'variables': 'tas,tasmax,tasmin,pr'
        }
        
        with open(config_file, 'w') as f:
            config.write(f)
        logger.info(f"Created default configuration file: {config_file}")
    else:
        config.read(config_file)
    
    return config

# Load configuration
CONFIG = load_configuration()

# Configuration - Load from config file with fallbacks
OUTPUT_DIR = CONFIG.get('paths', 'output_dir', fallback='output/climate_means')
PARALLEL_PROCESSING = CONFIG.getboolean('processing', 'parallel_processing', fallback=True)
EXTERNAL_DRIVE_PATH = CONFIG.get('paths', 'external_drive_path', 
                                fallback='/media/mihiarc/RPA1TB/data/NorESM2-LM')
ACTIVE_SCENARIO = CONFIG.get('processing', 'active_scenario', fallback='historical')
BATCH_SIZE = CONFIG.getint('processing', 'batch_size', fallback=20)
MAX_WORKERS = CONFIG.getint('processing', 'max_workers', fallback=4)

# Variables to process
VARIABLES = CONFIG.get('variables', 'variables', fallback='tas,tasmax,tasmin,pr').split(',')
VARIABLES = [v.strip() for v in VARIABLES]

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Memory monitoring utilities
def log_memory_usage(stage=""):
    """Log current memory usage."""
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024**2
    memory_gb = memory_mb / 1024
    logger.info(f"Memory usage {stage}: {memory_mb:.1f} MB ({memory_gb:.2f} GB)")

def validate_netcdf_structure(file_path: str, expected_vars: List[str]) -> bool:
    """
    Validate that NetCDF file has expected variables and dimensions.
    
    Args:
        file_path (str): Path to NetCDF file
        expected_vars (List[str]): List of expected variable names
        
    Returns:
        bool: True if file structure is valid
    """
    try:
        with xr.open_dataset(file_path) as ds:
            # Check for expected variables
            missing_vars = set(expected_vars) - set(ds.data_vars)
            if missing_vars:
                logger.warning(f"Missing variables in {file_path}: {missing_vars}")
                return False
            
            # Check for required dimensions
            required_dims = ['time', 'lat', 'lon']
            missing_dims = set(required_dims) - set(ds.dims)
            if missing_dims:
                logger.warning(f"Missing dimensions in {file_path}: {missing_dims}")
                return False
            
            # Check if time dimension has reasonable size
            if ds.dims.get('time', 0) < 1:
                logger.warning(f"Invalid time dimension size in {file_path}")
                return False
                
            return True
    except Exception as e:
        logger.error(f"Cannot read {file_path}: {e}")
        return False

def get_optimal_chunks(file_path):
    """Determine optimal chunk size based on data dimensions and available memory."""
    try:
        with xr.open_dataset(file_path, decode_times=False) as ds:
            time_size = ds.dims.get('time', 365)
            lat_size = ds.dims.get('lat', 100)
            lon_size = ds.dims.get('lon', 100)
            
            # Aim for ~50-100MB chunks for better performance
            target_chunk_size = 75 * 1024 * 1024  # 75MB
            element_size = 8  # 8 bytes for float64
            
            # Calculate optimal time chunking (at least 1 year, max 5 years)
            time_chunk = min(time_size, max(365, min(365 * 5, time_size // 5)))
            
            # Calculate if we need spatial chunking
            spatial_elements = lat_size * lon_size
            total_elements = spatial_elements * time_chunk
            
            if total_elements * element_size > target_chunk_size:
                # Need spatial chunking
                spatial_chunk = int(np.sqrt(target_chunk_size // (time_chunk * element_size)))
                spatial_chunk = max(10, min(spatial_chunk, 50))  # Reasonable bounds
                
                chunks = {
                    'time': time_chunk,
                    'lat': min(lat_size, spatial_chunk),
                    'lon': min(lon_size, spatial_chunk)
                }
            else:
                chunks = {'time': time_chunk}
            
            logger.debug(f"Optimal chunks for {Path(file_path).name}: {chunks}")
            return chunks
            
    except Exception as e:
        logger.warning(f"Could not determine optimal chunks for {file_path}: {e}")
        # Fallback to conservative chunking
        return {'time': 365, 'lat': 25, 'lon': 25}

def configure_dask_resources():
    """Configure Dask resources based on system capabilities and data characteristics."""
    total_memory = psutil.virtual_memory().total
    available_memory = psutil.virtual_memory().available
    
    logger.info(f"System memory - Total: {total_memory / 1024**3:.1f}GB, Available: {available_memory / 1024**3:.1f}GB")
    
    # Be conservative with memory allocation
    # Use at most 50% of available memory, min 2GB per worker, max 4GB per worker
    memory_per_worker = min(
        4 * 1024**3,  # 4GB max per worker
        max(2 * 1024**3, available_memory * 0.5 // 4)  # At least 2GB, up to 50% available / 4
    )
    
    # Calculate number of workers based on memory constraints
    max_workers_by_memory = int(available_memory * 0.5 // memory_per_worker)
    max_workers_by_cpu = max(1, mp.cpu_count() - 1)  # Leave one core free
    
    n_workers = min(max_workers_by_memory, max_workers_by_cpu, MAX_WORKERS)  # Cap at configured max
    
    # Allow some threading for I/O operations
    threads_per_worker = min(2, max(1, mp.cpu_count() // n_workers))
    
    config = {
        'n_workers': n_workers,
        'memory_limit': f'{int(memory_per_worker / 1024**3)}GB',
        'threads_per_worker': threads_per_worker
    }
    
    logger.info(f"Dask configuration: {config}")
    return config

def setup_dask_client():
    """Set up and return a Dask distributed client with optimized configuration."""
    logger.info("Initializing optimized Dask distributed computing...")
    
    dask_config = configure_dask_resources()
    
    cluster = LocalCluster(
        **dask_config,
        dashboard_address=':8787',
        silence_logs=logging.ERROR,  # Reduce log noise
        death_timeout='60s',  # Handle worker failures gracefully
        processes=True,  # Use processes for better isolation
    )
    
    client = Client(cluster)
    
    # Enable progress reporting
    ProgressBar().register()
    
    logger.info(f"Dask dashboard available at: {client.dashboard_link}")
    logger.info(f"Cluster info: {cluster}")
    
    return client, cluster

def open_dataset_optimized(file_path, chunks):
    """Open dataset with optimization for climate data."""
    return xr.open_dataset(
        file_path,
        chunks=chunks,
        engine='netcdf4',  # Usually faster than h5netcdf for large files
        decode_times=True,
        use_cftime=True,   # Handle climate calendars properly
        parallel=True,     # Enable parallel reading where possible
        cache=False        # Don't cache to save memory
    )

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
        'lon_min': 170,   # 170°E in 0-360 system (or -190°E when converted)
        'lon_max': 235,   # 235°E in 0-360 system (or -125°E when converted) - extended to include SE Alaska
        'lat_min': 50.0,
        'lat_max': 72.0,
        'convert_longitudes': True  # Convert to -180 to 180 for proper mapping
    },
    'HI': {
        'name': 'Hawaii and Islands',
        'lon_min': 181.63,   # 181.63 in 0-360 system
        'lon_max': 205.20,   # 205.20 in 0-360 system
        'lat_min': 18.92,
        'lat_max': 28.45,
        'convert_longitudes': True   # Convert to -180 to 180
    },
    'PRVI': {
        'name': 'Puerto Rico and U.S. Virgin Islands',
        'lon_min': 292.03,   # -67.97 in 0-360 system
        'lon_max': 295.49,   # -64.51 in 0-360 system
        'lat_min': 17.62,
        'lat_max': 18.57,
        'convert_longitudes': True   # Convert to -180 to 180
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

def generate_climate_periods(scenario: str, data_availability: Dict) -> List[Tuple[int, int, int, str]]:
    """
    Generate climate periods based on scenario.
    For each target year, creates a 30-year period ending in that year.
    
    Args:
        scenario (str): Climate scenario ('historical' or projection scenario)
        data_availability (Dict): Dictionary with data availability info
        
    Returns:
        List[Tuple[int, int, int, str]]: List of tuples (start_year, end_year, target_year, period_name)
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

def convert_longitude_bounds(lon_min: float, lon_max: float, is_0_360: bool) -> Dict[str, float]:
    """
    Convert longitude bounds between 0-360 and -180-180 coordinate systems.
    
    Args:
        lon_min (float): Minimum longitude in original system
        lon_max (float): Maximum longitude in original system
        is_0_360 (bool): True if data uses 0-360 system
        
    Returns:
        Dict[str, float]: Converted longitude bounds
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
    """Extract a specific region from the dataset with improved coordinate handling."""
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

def process_file_lazy_wrapper(file_path: str, variable_name: str, region_key: str, 
                             file_handler: ClimateFileHandler) -> Tuple[Optional[int], Optional[xr.DataArray]]:
    """Wrapper function to process a single NetCDF file with file_handler access."""
    try:
        # Validate file structure first
        if not validate_netcdf_structure(file_path, [variable_name]):
            logger.error(f"Invalid file structure: {file_path}")
            return None, None
        
        # Get optimal chunks for this file
        optimal_chunks = get_optimal_chunks(file_path)
        
        # Open with optimized settings but keep data lazy
        ds = open_dataset_optimized(file_path, optimal_chunks)
        
        # Extract region (still lazy)
        region_ds = extract_region(ds, REGION_BOUNDS[region_key])
        
        # Get the daily values (keep as dask array)
        daily_data = region_ds[variable_name]
        
        # Get year from filename using file_handler
        year = file_handler.extract_year_from_filename(file_path)
        
        return year, daily_data  # Return lazy dask array
        
    except Exception as e:
        logger.error(f"Error processing {file_path} for region {region_key}: {e}")
        return None, None

def compute_climate_normal_efficiently(data_arrays: List[xr.DataArray], years: List[int], 
                                     target_year: int) -> xr.DataArray:
    """Compute climate normal with optimized memory management and chunking."""
    logger.info(f"Computing climate normal for target year {target_year} using {len(years)} years of data")
    
    # Concatenate with proper dimension
    logger.info("Concatenating data arrays...")
    stacked_data = xr.concat(
        data_arrays, 
        dim=xr.DataArray(years, dims='year', name='year')
    )
    
    # Add dayofyear coordinate efficiently
    logger.info("Adding dayofyear coordinate...")
    dayofyear = stacked_data.time.dt.dayofyear
    stacked_data = stacked_data.assign_coords(dayofyear=('time', dayofyear))
    
    # Rechunk for optimal groupby operation
    logger.info("Rechunking for optimal groupby operation...")
    optimal_chunks = {
        'year': len(years),  # Keep all years together for groupby
        'lat': 25,
        'lon': 25
    }
    
    # Only rechunk if current chunking is significantly different
    current_chunks = stacked_data.chunks
    if 'year' not in current_chunks or current_chunks.get('year', [0])[0] != len(years):
        stacked_data = stacked_data.chunk(optimal_chunks)
    
    # Compute climate average using groupby (still lazy)
    logger.info("Computing climate average...")
    climate_avg = stacked_data.groupby('dayofyear').mean(dim=['year'])
    
    # Create new time coordinates using the target year as reference
    days = climate_avg.dayofyear.values
    dates = [np.datetime64(f'{target_year}-01-01') + np.timedelta64(int(d-1), 'D') for d in days]
    
    # Assign new time coordinate
    climate_avg = climate_avg.assign_coords(time=('dayofyear', dates))
    climate_avg = climate_avg.swap_dims({'dayofyear': 'time'})
    
    # Add metadata about the climate period
    climate_avg.attrs.update({
        'climate_period_start': min(years),
        'climate_period_end': max(years),
        'climate_target_year': target_year,
        'climate_period_length': len(years),
        'description': f'Climate normal for {target_year}, based on {min(years)}-{max(years)}'
    })
    
    return climate_avg  # Return lazy array

def process_period_region_optimized(period_info: Tuple[int, int, int, str], variable_name: str, 
                                  file_handler: ClimateFileHandler, region_key: str) -> Optional[xr.DataArray]:
    """Process a climate period for a specific variable and region - optimized version."""
    start_year, end_year, target_year, period_name = period_info
    region_name = REGION_BOUNDS[region_key]['name']
    
    # Calculate the number of years in the period
    period_length = end_year - start_year + 1
    
    logger.info(f"Processing {variable_name} for {period_name}")
    logger.info(f"{period_length}-year period: {start_year}-{end_year} (target: {target_year})")
    logger.info(f"Region: {region_name}")
    
    if period_length != 30:
        logger.warning(f"Period length ({period_length} years) is not the standard 30 years")
    
    # Extract scenario from period_name
    scenario = period_name.split('_')[0]
    
    # Get all files for this period using the file handler
    files = file_handler.get_files_for_period(variable_name, scenario, start_year, end_year)
    
    if not files:
        logger.warning(f"No files found for {variable_name} during {period_name}")
        return None
    
    # Process files using dask delayed (keep everything lazy)
    logger.info(f"Creating delayed tasks for {len(files)} files...")
    delayed_results = []
    for f in files:
        # Use wrapper function that has access to file_handler
        delayed_result = dask.delayed(process_file_lazy_wrapper)(f, variable_name, region_key, file_handler)
        delayed_results.append(delayed_result)
    
    # Compute delayed tasks to get data arrays (but keep arrays lazy)
    logger.info("Computing file processing tasks...")
    results = dask.compute(*delayed_results)
    
    # Filter out None results
    valid_results = [r for r in results if r[0] is not None]
    
    if not valid_results:
        logger.warning(f"No valid data for {variable_name} during {period_name} in {region_name}")
        return None
    
    # Extract years and data arrays
    years = [year for year, _ in valid_results]
    data_arrays = [data for _, data in valid_results]
    
    logger.info(f"Processing years: {sorted(years)} (total: {len(years)} years)")
    
    if len(years) < period_length:
        logger.warning(f"Only found {len(years)} years of data for {period_length}-year period")
        missing_years = set(range(start_year, end_year + 1)) - set(years)
        logger.warning(f"Missing years: {sorted(missing_years)}")
    
    # Compute climate normal efficiently (returns lazy array)
    climate_avg = compute_climate_normal_efficiently(data_arrays, years, target_year)
    
    # Apply unit conversions (still lazy)
    if variable_name in ['tas', 'tasmax', 'tasmin']:
        # Convert from Kelvin to Celsius
        climate_avg = climate_avg - 273.15
        climate_avg.attrs['units'] = 'degC'
    elif variable_name == 'pr':
        # Convert from kg m^-2 s^-1 to mm/day
        climate_avg = climate_avg * 86400
        climate_avg.attrs['units'] = 'mm/day'
    
    return climate_avg  # Return lazy array

def convert_longitudes_to_standard(ds: xr.Dataset) -> xr.Dataset:
    """
    Convert longitudes from 0-360 format to standard -180 to 180 format.
    
    Args:
        ds (xarray.Dataset): Dataset with longitude coordinates in 0-360 format
        
    Returns:
        xarray.Dataset: Dataset with longitude coordinates in -180 to 180 format
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

def save_region_dataset_optimized(region_key: str, dataset: xr.Dataset, scenario: str) -> str:
    """Save region dataset with optimized I/O and proper formatting."""
    region_name = REGION_BOUNDS[region_key]['name'].lower().replace(' ', '_')
    
    # Include scenario in output filename with descriptive naming
    if scenario == 'historical':
        output_file = os.path.join(OUTPUT_DIR, f'{region_name}_{scenario}_climate_means_2012-2014.nc')
    else:
        output_file = os.path.join(OUTPUT_DIR, f'{region_name}_{scenario}_climate_means_2015-2100.nc')
    
    logger.info(f"Preparing to save {region_name} data to {output_file}...")
    
    # Convert longitudes if specified in the region settings
    if REGION_BOUNDS[region_key].get('convert_longitudes', False):
        logger.info(f"Converting longitudes for {region_name} from 0-360 to -180 to 180 format...")
        dataset = convert_longitudes_to_standard(dataset)
    
    # Add global attributes to the dataset
    dataset.attrs.update({
        'title': f'Climate Means for {REGION_BOUNDS[region_key]["name"]}',
        'scenario': scenario,
        'methodology': 'Annual climate means',
        'temporal_coverage': '2012-2014' if scenario == 'historical' else '2015-2100',
        'spatial_coverage': REGION_BOUNDS[region_key]['name'],
        'created': datetime.now().isoformat(),
        'description': 'Annual climate measures based on available years.',
        'variables': 'pr (precipitation), tas (temperature), tasmax (max temperature), tasmin (min temperature)',
        'units_precipitation': 'mm/day',
        'units_temperature': 'degC'
    })
    
    # Optimize encoding for NetCDF output
    encoding = {}
    for var in dataset.data_vars:
        encoding[var] = {
            'zlib': True,          # Enable compression
            'complevel': 4,        # Moderate compression level
            'shuffle': True,       # Enable shuffle filter
            'chunksizes': None,    # Let xarray determine optimal chunks for storage
            'fletcher32': False,   # Disable checksum for speed
        }
    
    # Save the dataset with optimized settings
    logger.info(f"Computing and saving {region_name} data...")
    log_memory_usage("before saving")
    
    dataset.to_netcdf(
        output_file,
        encoding=encoding,
        format='NETCDF4',
        engine='netcdf4'
    )
    
    log_memory_usage("after saving")
    logger.info(f"✓ Saved {region_name} data to {output_file}")
    logger.info(f"  Variables: {list(dataset.data_vars.keys())}")
    
    # Get the count of variables for each type
    var_counts = {}
    for var_name in dataset.data_vars.keys():
        var_type = var_name.split('_')[0]  # Extract variable type (e.g., 'pr', 'tas')
        var_counts[var_type] = var_counts.get(var_type, 0) + 1
    
    logger.info(f"  Variable counts: {var_counts}")
    
    return output_file

def main():
    """Main processing function with optimized Dask implementation."""
    start_time = datetime.now()
    logger.info(f"Starting enhanced climate data processing at {start_time}")
    log_memory_usage("startup")
    
    # Initialize file handler
    file_handler = create_file_handler(
        external_drive_path=EXTERNAL_DRIVE_PATH,
        variables=VARIABLES,
        scenarios=[ACTIVE_SCENARIO]  # Only process active scenario
    )
    
    # Validate file paths
    if not file_handler.validate_base_path():
        logger.error("File path validation failed. Please check your configuration.")
        logger.info(file_handler.get_expected_file_structure_info())
        return
    
    # Discover available files
    logger.info("Discovering available files...")
    available_files = file_handler.discover_available_files()
    log_memory_usage("after file discovery")
    
    # Set up optimized dask distributed client
    client, cluster = setup_dask_client()
    log_memory_usage("after Dask setup")
    
    try:
        scenario = ACTIVE_SCENARIO
        logger.info(f"Processing scenario: {scenario}")
        
        # Data availability from file handler
        data_availability = file_handler._data_availability
        
        # Generate climate periods for the active scenario
        climate_periods = generate_climate_periods(scenario, data_availability)
        
        if not climate_periods:
            logger.error(f"No climate periods generated for scenario {scenario}")
            return
        
        # Get available variables for this scenario
        available_vars = list(available_files.get(scenario, {}).keys())
        processing_vars = [var for var in VARIABLES if var in available_vars]
        
        if not processing_vars:
            logger.error(f"No variables available for processing in scenario {scenario}")
            logger.info(f"Available variables: {available_vars}")
            return
        
        logger.info(f"Processing variables: {processing_vars}")
        
        # Collect all lazy computations organized by region
        region_lazy_datasets = {}
        computation_tasks = []
        
        logger.info("Creating lazy computation graph...")
        log_memory_usage("before creating computation graph")
        
        # Process each variable and period to build lazy computation graph
        for var_name in processing_vars:
            logger.info(f"Setting up lazy computations for {var_name}...")
            
            # Process each climate period
            for period in climate_periods:
                start_year, end_year, target_year, period_name = period
                
                # Process each region
                for region_key in REGION_BOUNDS.keys():
                    region_name = REGION_BOUNDS[region_key]['name']
                    
                    # Create lazy computation for this combination
                    lazy_result = dask.delayed(process_period_region_optimized)(
                        period, var_name, file_handler, region_key
                    )
                    
                    # Store the task for batch computation
                    task_key = f"{region_key}_{var_name}_{target_year}"
                    computation_tasks.append((task_key, lazy_result))
                    
                    # Initialize region dataset collection if needed
                    if region_key not in region_lazy_datasets:
                        region_lazy_datasets[region_key] = {}
        
        logger.info(f"Created {len(computation_tasks)} lazy computation tasks")
        log_memory_usage("after creating computation graph")
        
        # Compute all tasks in batches to manage memory
        batch_size = min(BATCH_SIZE, len(computation_tasks))  # Use configured batch size
        logger.info(f"Computing tasks in batches of {batch_size}...")
        
        total_batches = (len(computation_tasks) + batch_size - 1) // batch_size
        
        # Use tqdm for progress tracking
        for i in tqdm(range(0, len(computation_tasks), batch_size), 
                     desc="Processing batches", 
                     total=total_batches):
            batch = computation_tasks[i:i + batch_size]
            batch_keys = [key for key, _ in batch]
            batch_tasks = [task for _, task in batch]
            
            current_batch = i // batch_size + 1
            logger.info(f"Computing batch {current_batch}/{total_batches}")
            logger.info(f"Batch contains tasks: {batch_keys[:3]}{'...' if len(batch_keys) > 3 else ''}")
            
            log_memory_usage(f"before batch {current_batch}")
            
            # Compute this batch
            batch_results = dask.compute(*batch_tasks)
            
            log_memory_usage(f"after computing batch {current_batch}")
            
            # Organize results by region
            for (task_key, _), result in zip(batch, batch_results):
                if result is not None:
                    region_key, var_name, target_year = task_key.split('_', 2)
                    
                    if region_key not in region_lazy_datasets:
                        region_lazy_datasets[region_key] = {}
                    
                    var_id = f"{var_name}_{target_year}"
                    region_lazy_datasets[region_key][var_id] = result
                    
                    logger.debug(f"Added {var_name} for target year {target_year} in {REGION_BOUNDS[region_key]['name']}")
            
            # Trigger garbage collection between batches
            gc.collect()
            log_memory_usage(f"after cleanup batch {current_batch}")
        
        # Convert collections to xarray Datasets and save
        logger.info("Creating final datasets and saving...")
        log_memory_usage("before creating final datasets")
        
        saved_files = []
        for region_key, var_dict in region_lazy_datasets.items():
            if var_dict:  # Only process regions with data
                region_name = REGION_BOUNDS[region_key]['name']
                logger.info(f"Creating dataset for {region_name} with {len(var_dict)} variables")
                
                log_memory_usage(f"before creating {region_name} dataset")
                
                # Create xarray Dataset
                region_dataset = xr.Dataset(var_dict)
                
                # Save with optimized I/O
                output_file = save_region_dataset_optimized(region_key, region_dataset, scenario)
                saved_files.append(output_file)
                
                # Clean up memory
                del region_dataset
                gc.collect()
                log_memory_usage(f"after saving {region_name}")
            else:
                logger.warning(f"No data collected for region {REGION_BOUNDS[region_key]['name']}")
        
        # Final summary
        end_time = datetime.now()
        runtime = end_time - start_time
        logger.info(f"Processing complete!")
        logger.info(f"Total runtime: {runtime}")
        logger.info(f"Saved {len(saved_files)} output files:")
        for file_path in saved_files:
            logger.info(f"  {file_path}")
        
        # Display final memory and performance statistics
        log_memory_usage("final")
        logger.info(f"Peak memory usage: {psutil.Process().memory_info().rss / 1024**3:.2f} GB")
        
        # Calculate processing rate
        total_tasks = len(computation_tasks)
        tasks_per_minute = total_tasks / (runtime.total_seconds() / 60)
        logger.info(f"Processing rate: {tasks_per_minute:.1f} tasks/minute")
        
    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)
        log_memory_usage("error state")
        raise
    
    finally:
        # Comprehensive cleanup
        logger.info("Cleaning up Dask resources...")
        
        try:
            # Clear any remaining computations
            client.restart()
            client.close()
            cluster.close()
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
        
        # Final memory cleanup
        gc.collect()
        log_memory_usage("after cleanup")
        
        final_time = datetime.now()
        total_runtime = final_time - start_time
        logger.info(f"Total runtime including cleanup: {total_runtime}")

def run_diagnostics():
    """Run system diagnostics and configuration validation."""
    logger.info("=== System Diagnostics ===")
    
    # System information
    logger.info(f"Platform: {platform.system()} {platform.release()}")
    logger.info(f"Python version: {platform.python_version()}")
    
    # Memory information
    memory = psutil.virtual_memory()
    logger.info(f"Total memory: {memory.total / 1024**3:.1f} GB")
    logger.info(f"Available memory: {memory.available / 1024**3:.1f} GB")
    logger.info(f"Memory usage: {memory.percent:.1f}%")
    
    # CPU information
    logger.info(f"CPU cores: {mp.cpu_count()}")
    logger.info(f"CPU usage: {psutil.cpu_percent(interval=1):.1f}%")
    
    # Disk space
    disk = psutil.disk_usage(OUTPUT_DIR)
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info(f"Disk space available: {disk.free / 1024**3:.1f} GB")
    
    # Configuration
    logger.info("=== Configuration ===")
    logger.info(f"External drive path: {EXTERNAL_DRIVE_PATH}")
    logger.info(f"Active scenario: {ACTIVE_SCENARIO}")
    logger.info(f"Variables: {VARIABLES}")
    logger.info(f"Parallel processing: {PARALLEL_PROCESSING}")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    
    # File handler validation
    logger.info("=== File Handler Validation ===")
    try:
        file_handler = create_file_handler(
            external_drive_path=EXTERNAL_DRIVE_PATH,
            variables=VARIABLES,
            scenarios=[ACTIVE_SCENARIO]
        )
        
        if file_handler.validate_base_path():
            logger.info("✓ Base path validation passed")
            
            # Quick file discovery
            available_files = file_handler.discover_available_files()
            for scenario, vars_dict in available_files.items():
                for var, file_list in vars_dict.items():
                    if file_list:
                        years = [year for year, _ in file_list]
                        logger.info(f"✓ {scenario}/{var}: {len(file_list)} files ({min(years)}-{max(years)})")
                    else:
                        logger.warning(f"✗ {scenario}/{var}: No files found")
        else:
            logger.error("✗ Base path validation failed")
            
    except Exception as e:
        logger.error(f"File handler validation failed: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Climate Data Processor')
    parser.add_argument('--diagnostics', action='store_true', 
                       help='Run system diagnostics only')
    parser.add_argument('--config', type=str, 
                       help='Path to configuration file')
    parser.add_argument('--scenario', type=str, 
                       help='Override active scenario')
    parser.add_argument('--variables', type=str, 
                       help='Comma-separated list of variables to process')
    
    args = parser.parse_args()
    
    # Then override configuration with command line arguments
    if args.scenario:
        ACTIVE_SCENARIO = args.scenario
        logger.info(f"Scenario overridden to: {ACTIVE_SCENARIO}")
    
    if args.variables:
        VARIABLES = [v.strip() for v in args.variables.split(',')]
        logger.info(f"Variables overridden to: {VARIABLES}")
    
    if args.diagnostics:
        run_diagnostics()
    else:
        main()