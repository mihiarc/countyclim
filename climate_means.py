#!/usr/bin/env python3
"""
Climate Data Processing Script

Processes daily temperature and precipitation NetCDF files to calculate
annual climate averages for US territories.
"""

import os
import xarray as xr
import numpy as np
import glob
from datetime import datetime
import multiprocessing as mp
import dask
from dask.distributed import Client, LocalCluster

# Configuration - ADJUST THESE PATHS AND SETTINGS FOR YOUR ENVIRONMENT
TEMP_FILE_PATTERN = 'data/tas/tas_day_*.nc'  # Updated file pattern
PRECIP_FILE_PATTERN = 'data/pr/pr_day_*.nc'  # Updated file pattern
OUTPUT_DIR = 'output/climate_means'  # Output directory
PARALLEL_PROCESSING = True  # Set to False for serial processing
MAX_PROCESSES = mp.cpu_count() - 1  # Number of processes to use

# Dask configuration
CHUNK_SIZE = {'time': 365}  # Chunk by one year of daily data
MEMORY_LIMIT = '64GB'  # Adjust based on your system's available memory

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

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

# Define region-specific CRS settings
def get_region_crs_info(region_key):
    """
    Get coordinate reference system information for a specific region.
    
    This function provides region-specific CRS settings for proper spatial data handling,
    especially useful for visualization and advanced spatial analysis.
    
    Args:
        region_key (str): Region identifier (CONUS, AK, HI, PRVI, GU)
        
    Returns:
        dict: Dictionary with CRS information including:
            - crs_type: Type of CRS specification ('epsg', 'proj4', or 'name')
            - crs_value: The EPSG code, proj4 string, or projection name
            - central_longitude: Central longitude for the projection (for visualization)
            - central_latitude: Central latitude for the projection (for visualization)
            - extent: Recommended map extent in degrees [lon_min, lon_max, lat_min, lat_max]
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

# Configuration - ADJUST THESE PATHS AND SETTINGS FOR YOUR ENVIRONMENT
SCENARIOS = {
    'historical': {
        'tas': 'data/tas/historical/tas_day_*.nc',
        'tasmax': 'data/tasmax/historical/tasmax_day_*.nc',
        'tasmin': 'data/tasmin/historical/tasmin_day_*.nc',
        'pr': 'data/pr/historical/pr_day_*.nc'
    },
    'ssp126': { 
        'tas': 'data/tas/ssp126/tas_day_*.nc',
        'tasmax': 'data/tasmax/ssp126/tasmax_day_*.nc',
        'tasmin': 'data/tasmin/ssp126/tasmin_day_*.nc',
        'pr': 'data/pr/ssp126/pr_day_*.nc'
    },
    'ssp245': { 
        'tas': 'data/tas/ssp245/tas_day_*.nc',
        'tasmax': 'data/tasmax/ssp245/tasmax_day_*.nc',
        'tasmin': 'data/tasmin/ssp245/tasmin_day_*.nc',
        'pr': 'data/pr/ssp245/pr_day_*.nc'
    },
    'ssp370': { 
        'tas': 'data/tas/ssp370/tas_day_*.nc',
        'tasmax': 'data/tasmax/ssp370/tasmax_day_*.nc',
        'tasmin': 'data/tasmin/ssp370/tasmin_day_*.nc',
        'pr': 'data/pr/ssp370/pr_day_*.nc'
    },
    'ssp585': { 
        'tas': 'data/tas/ssp585/tas_day_*.nc',
        'tasmax': 'data/tasmax/ssp585/tasmax_day_*.nc',
        'tasmin': 'data/tasmin/ssp585/tasmin_day_*.nc',
        'pr': 'data/pr/ssp585/pr_day_*.nc'
    }
}

# Active scenario for processing
ACTIVE_SCENARIO = 'historical'  # Change this to process different scenarios

# Define climate periods
CLIMATE_PERIODS = [
    (2012, 2014, '2012-2014'),  # Historical baseline
    # Future periods - uncomment when processing future scenarios:
    # (2036, 2065, '2050s'),    # Mid-century
    # (2071, 2100, '2080s')     # End-century
]

def get_year_from_filename(filename):
    """Extract year from filename."""
    base = os.path.basename(filename)
    
    # For tas files: tas_day_NorESM2-LM_historical_r1i1p1f1_gn_2012.nc
    # For pr files: pr_day_NorESM2-LM_historical_r1i1p1f1_gn_2012_v1.1.nc
    parts = base.split('_')
    
    # Look for year in parts that could contain it
    for part in parts:
        # Check if this part starts with '20' (for years 2012, 2013, etc.)
        if part.startswith('20') and len(part) >= 4:
            # Extract the first 4 characters if the part has additional content
            year_str = part[:4]
            try:
                return int(year_str)
            except ValueError:
                continue
    
    print(f"Warning: Could not extract year from {filename}")
    return None

def get_files_for_period(file_pattern, start_year, end_year):
    """Get all files for a given climate period."""
    all_files = glob.glob(file_pattern)
    period_files = []
    
    for f in all_files:
        year = get_year_from_filename(f)
        if year and start_year <= year <= end_year:
            period_files.append(f)
    
    if not period_files:
        print(f"Warning: No files found matching pattern {file_pattern} for years {start_year}-{end_year}")
    else:
        print(f"Found {len(period_files)} files for period {start_year}-{end_year}")
        
    return sorted(period_files)

def extract_region(ds, region_bounds):
    """Extract a specific region from the dataset."""
    # Check coordinate names
    lon_name = 'lon' if 'lon' in ds.coords else 'x'
    lat_name = 'lat' if 'lat' in ds.coords else 'y'
    
    # Check longitude range (0-360 or -180-180)
    lon_min = ds[lon_name].min().item()
    lon_max = ds[lon_name].max().item()
    
    # Determine if we're using 0-360 or -180-180 coordinate system
    is_0_360 = lon_min >= 0 and lon_max > 180
    
    # Convert region bounds to match the dataset's coordinate system
    if is_0_360:
        # Already in 0-360 system, use bounds as is
        lon_bounds = {
            'lon_min': region_bounds['lon_min'],
            'lon_max': region_bounds['lon_max']
        }
    else:
        # Convert from 0-360 to -180-180 system
        lon_bounds = {
            'lon_min': region_bounds['lon_min'] - 360 if region_bounds['lon_min'] > 180 else region_bounds['lon_min'],
            'lon_max': region_bounds['lon_max'] - 360 if region_bounds['lon_max'] > 180 else region_bounds['lon_max']
        }
    
    # Handle the case where we cross the 0/360 or -180/180 boundary
    if lon_bounds['lon_min'] > lon_bounds['lon_max']:
        if is_0_360:
            region_ds = ds.where(
                ((ds[lon_name] >= lon_bounds['lon_min']) | 
                 (ds[lon_name] <= lon_bounds['lon_max'])) & 
                (ds[lat_name] >= region_bounds['lat_min']) & 
                (ds[lat_name] <= region_bounds['lat_max']), 
                drop=True
            )
        else:
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
        print(f"Warning: No data found within region bounds after filtering.")
        print(f"Dataset longitude range: {lon_min} to {lon_max}")
        print(f"Region bounds: {region_bounds['lon_min']} to {region_bounds['lon_max']} (original)")
        if is_0_360:
            print(f"Region bounds (in 0-360): {lon_bounds['lon_min']} to {lon_bounds['lon_max']}")
        else:
            print(f"Region bounds (in -180-180): {lon_bounds['lon_min']} to {lon_bounds['lon_max']}")
    
    return region_ds

def process_file(file_path, variable_name, region_key):
    """Process a single NetCDF file to get daily values for a specific region."""
    try:
        # Open the file with dask chunking
        ds = xr.open_dataset(file_path, chunks=CHUNK_SIZE)
        
        # Extract region
        region_ds = extract_region(ds, REGION_BOUNDS[region_key])
        
        # Get the daily values (no averaging) - keep as dask array
        daily_data = region_ds[variable_name]
        
        # Get year from filename
        year = get_year_from_filename(file_path)
        
        return year, daily_data
        
    except Exception as e:
        print(f"Error processing {file_path} for region {region_key}: {e}")
        return None, None

def process_period_region(period_info, variable_name, file_pattern, region_key):
    """Process a climate period for a specific variable and region."""
    start_year, end_year, period_name = period_info
    region_name = REGION_BOUNDS[region_key]['name']
    print(f"Processing {variable_name} for period {period_name} in {region_name}...")
    
    # Get all files for this period
    files = get_files_for_period(file_pattern, start_year, end_year)
    
    if not files:
        print(f"No files found for {variable_name} during {period_name}")
        return None
    
    # Process files using dask delayed
    results = []
    for f in files:
        year, data = process_file(f, variable_name, region_key)
        if year is not None:
            results.append((year, data))
    
    if not results:
        print(f"No valid data for {variable_name} during {period_name} in {region_name}")
        return None
    
    # Extract the data arrays and align them by day of year
    years = [year for year, _ in results]
    print(f"Processing years: {years}")
    
    # First, ensure all arrays have dayofyear as a coordinate
    data_arrays = []
    for _, data in results:
        # Calculate dayofyear while preserving original structure
        dayofyear = data.time.dt.dayofyear.data  # Extract the underlying numpy array
        # Add dayofyear as a coordinate without changing dimensions
        with_doy = data.assign_coords(dayofyear=('time', dayofyear))
        data_arrays.append(with_doy)
    
    # Stack the arrays along a new 'year' dimension
    print("Concatenating data arrays...")
    stacked_data = xr.concat(data_arrays, dim=xr.DataArray(years, dims='year', name='year'))
    print(f"Stacked data dimensions: {stacked_data.dims}")
    
    # First compute mean over years for each unique dayofyear
    print("Computing climate averages...")
    # Group by dayofyear and compute mean, reducing both year and time dimensions
    climate_avg = stacked_data.groupby('dayofyear').mean(dim=['year', 'time'])
    print(f"Climate average dimensions: {climate_avg.dims}")
    
    # Create new time coordinates
    reference_year = 2000
    days = climate_avg.dayofyear.values
    dates = [np.datetime64(f'{reference_year}-01-01') + np.timedelta64(int(d-1), 'D') for d in days]
    
    # Assign new time coordinate using dayofyear as the dimension
    climate_avg = climate_avg.assign_coords(time=('dayofyear', dates))
    climate_avg = climate_avg.swap_dims({'dayofyear': 'time'})
    print(f"Final dimensions: {climate_avg.dims}")
    
    return climate_avg

def convert_longitudes_to_standard(ds):
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
        print("Warning: 'lon' coordinate not found, skipping longitude conversion")
        return ds_converted
    
    # Convert longitudes that are > 180 to negative values (0-360 → -180 to 180)
    lon_values = ds_converted.lon.values
    new_lon_values = np.where(lon_values > 180, lon_values - 360, lon_values)
    
    # Update the longitude coordinate
    ds_converted = ds_converted.assign_coords(lon=new_lon_values)
    
    # Sort by the new longitudes to maintain increasing order
    ds_converted = ds_converted.sortby('lon')
    
    return ds_converted

def main():
    """Main processing function."""
    start_time = datetime.now()
    print(f"Starting climate data processing at {start_time}")
    print(f"Processing scenario: {ACTIVE_SCENARIO}")
    
    # Initialize dask client for distributed computing
    cluster = LocalCluster(n_workers=MAX_PROCESSES, memory_limit=MEMORY_LIMIT)
    client = Client(cluster)
    print(f"Dask dashboard available at: {client.dashboard_link}")
    
    try:
        # Initialize datasets for each region
        region_datasets = {}
        
        # Get the file patterns for the active scenario
        scenario_patterns = SCENARIOS[ACTIVE_SCENARIO]
        
        # Process each variable
        for var_name, file_pattern in scenario_patterns.items():
            print(f"\nProcessing {var_name} data...")
            
            # Process each climate period
            for period in CLIMATE_PERIODS:
                period_name = period[2]
                
                # Process each region
                for region_key in REGION_BOUNDS.keys():
                    region_name = REGION_BOUNDS[region_key]['name']
                    
                    # Calculate climate average for this period, variable, and region
                    climate_avg = process_period_region(period, var_name, file_pattern, region_key)
                    
                    if climate_avg is not None:
                        # --- UNIT CONVERSIONS ---
                        if var_name in ['tas', 'tasmax', 'tasmin']:
                            # Convert from Kelvin to Celsius - maintain lazy evaluation
                            climate_avg = climate_avg - 273.15
                            climate_avg.attrs['units'] = 'degC'
                        elif var_name == 'pr':
                            # Convert from kg m^-2 s^-1 to mm/day - maintain lazy evaluation
                            climate_avg = climate_avg * 86400
                            climate_avg.attrs['units'] = 'mm/day'
                        # ------------------------
                        # Initialize dataset for this region if it doesn't exist
                        if region_key not in region_datasets:
                            region_datasets[region_key] = xr.Dataset()
                        
                        # Add to region dataset
                        var_id = f"{var_name}_{period_name}"
                        region_datasets[region_key][var_id] = climate_avg
                        print(f"Added {var_name} for {period_name} in {region_name} to output dataset")
        
        # Save separate files for each region
        for region_key, ds in region_datasets.items():
            region_name = REGION_BOUNDS[region_key]['name'].lower().replace(' ', '_')
            
            # Include scenario in output filename
            output_file = os.path.join(OUTPUT_DIR, f'{region_name}_{ACTIVE_SCENARIO}_climate_averages.nc')
            
            # Convert longitudes if specified in the region settings
            if REGION_BOUNDS[region_key].get('convert_longitudes', False):
                print(f"Converting longitudes for {region_name} from 0-360 to -180 to 180 format...")
                ds = convert_longitudes_to_standard(ds)
            
            # Save the dataset - trigger computation
            print(f"Computing and saving {region_name} data...")
            ds = ds.compute()  # Explicitly compute before saving
            ds.to_netcdf(output_file)
            print(f"Saved {region_name} data to {output_file}")
        
        end_time = datetime.now()
        runtime = end_time - start_time
        print(f"\nProcessing complete!")
        print(f"Total runtime: {runtime}")
        
        # Output data summary
        print("\nOutput datasets summary:")
        for region_key, ds in region_datasets.items():
            print(f"\n{REGION_BOUNDS[region_key]['name']}:")
            print(ds)
            
    finally:
        # Clean up dask client and cluster
        client.close()
        cluster.close()

if __name__ == "__main__":
    main()