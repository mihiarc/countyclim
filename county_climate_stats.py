#!/usr/bin/env python3
"""
county_climate_stats.py

Script to calculate county-level climate statistics:
1. Annual mean temperature
2. Annual count of days with maximum temperature > 90°F (32.22°C)
3. Annual count of days with minimum temperature < 32°F (0°C)
4. Total annual precipitation
5. Annual count of days with precipitation > 1 inch (25.4mm)
"""

import os
import xarray as xr
import numpy as np
import pandas as pd
import rasterio
from rasterstats import zonal_stats
import geopandas as gpd
import logging
from datetime import datetime
import warnings
import multiprocessing as mp
import dask
from dask.distributed import Client, LocalCluster

warnings.filterwarnings('ignore', category=RuntimeWarning)

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Dask configuration
CHUNK_SIZE = {'time': 365}  # Chunk by one year of daily data
MAX_PROCESSES = mp.cpu_count() - 1  # Number of processes to use
MEMORY_LIMIT = '64GB'  # Adjust based on your system's available memory

# Define climate thresholds
THRESHOLDS = {
    'tasmax_hot': 32.22,    # 90°F in Celsius
    'tasmin_cold': 0.0,     # 32°F in Celsius
    'pr_heavy': 25.4        # 1 inch in mm
}

def load_climate_data(climate_data_path):
    """
    Load climate data from NetCDF file.
    
    Args:
        climate_data_path (str): Path to the climate data NetCDF file
        
    Returns:
        xarray.Dataset: Climate data
    """
    logger.info(f"Loading climate data from {climate_data_path}")
    ds = xr.open_dataset(climate_data_path, chunks=CHUNK_SIZE)
    
    logger.info(f"Climate variables: {list(ds.data_vars)}")
    logger.info(f"Dimensions: {ds.dims}")
    logger.info(f"Time range: {ds.time.values[0]} to {ds.time.values[-1]}")
    logger.info(f"Chunk sizes: {ds.chunks}")
    
    return ds

def load_county_boundaries(boundaries_path):
    """
    Load county boundaries from parquet file.
    
    Args:
        boundaries_path (str): Path to the county boundaries parquet file
        
    Returns:
        geopandas.GeoDataFrame: County boundaries
    """
    logger.info(f"Loading county boundaries from {boundaries_path}")
    counties = gpd.read_parquet(boundaries_path)
    
    logger.info(f"Loaded {len(counties)} county boundaries")
    logger.info(f"CRS: {counties.crs}")
    
    # Create id_numeric if needed
    if 'id_numeric' not in counties.columns and 'GEOID' in counties.columns:
        logger.info("Creating id_numeric field from GEOID")
        counties['id_numeric'] = counties.index + 1
    
    # Create STUSPS (state postal code) from STATEFP if needed
    if 'STUSPS' not in counties.columns and 'STATEFP' in counties.columns:
        logger.info("Creating STUSPS field from STATEFP")
        # FIPS to postal code mapping
        state_fips_to_postal = {
            '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA',
            '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL',
            '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN',
            '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME',
            '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS',
            '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH',
            '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND',
            '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
            '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT',
            '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI',
            '56': 'WY'
        }
        counties['STUSPS'] = counties['STATEFP'].map(state_fips_to_postal)
        logger.info("Added STUSPS column")
    
    return counties

def compute_daily_zonal_stats(climate_data, counties, variable, stat_type, output_dir):
    """
    Compute zonal statistics for each county for each day using Dask for parallel processing.
    
    Args:
        climate_data (xarray.Dataset): Climate data
        counties (geopandas.GeoDataFrame): County boundaries
        variable (str): Climate variable to analyze
        stat_type (str): Type of statistic to compute ('mean', 'sum', or 'threshold')
        output_dir (str): Directory to save results
        
    Returns:
        pandas.DataFrame: Daily zonal statistics for each county
    """
    logger.info(f"Computing daily zonal statistics for {variable}")
    
    var_data = climate_data[variable]
    
    # Check if we need to convert precipitation units
    if variable.startswith('pr') and 'units' in var_data.attrs:
        current_units = var_data.attrs['units']
        logger.info(f"Precipitation data units: {current_units}")
        if current_units == 'kg m-2 s-1':
            logger.info("Converting precipitation from kg/m²/s to mm/day")
            var_data = var_data * 86400
            var_data.attrs['units'] = 'mm/day'
        elif current_units != 'mm/day':
            logger.warning(f"Unexpected precipitation units: {current_units}")
    
    lon_values = var_data.lon.values
    lat_values = var_data.lat.values
    
    # Calculate grid cell size
    if len(lon_values) > 1:
        dx = lon_values[1] - lon_values[0]
    else:
        dx = 1.0  # Default to 1 degree for single cell
    if len(lat_values) > 1:
        dy = lat_values[1] - lat_values[0]
    else:
        dy = 1.0  # Default to 1 degree for single cell
    
    transform = rasterio.transform.from_bounds(
        np.min(lon_values) - dx/2, 
        np.min(lat_values) - dy/2, 
        np.max(lon_values) + dx/2, 
        np.max(lat_values) + dy/2, 
        len(lon_values), 
        len(lat_values)
    )
    
    # Create a temporary directory for raster files
    temp_dir = os.path.join(output_dir, 'temp_rasters')
    os.makedirs(temp_dir, exist_ok=True)
    
    def process_day(data_array, day_idx):
        """Process a single day of data."""
        data_to_write = np.flipud(data_array.values)
        temp_raster = os.path.join(temp_dir, f"{variable}_temp_{day_idx}.tif")
        
        try:
            with rasterio.open(
                temp_raster, 
                'w', 
                driver='GTiff', 
                height=data_to_write.shape[0], 
                width=data_to_write.shape[1], 
                count=1, 
                dtype=data_to_write.dtype, 
                crs='+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs', 
                transform=transform
            ) as dst:
                dst.write(data_to_write, 1)
            
            # For precipitation, use mean value within each county to avoid bias from county size
            if variable.startswith('pr'):
                stats = zonal_stats(counties, temp_raster, stats=['mean'])
                daily_values = [s['mean'] for s in stats]
            else:
                stats = zonal_stats(counties, temp_raster, stats=[stat_type])
                daily_values = [s[stat_type] for s in stats]
            
            return daily_values
            
        finally:
            if os.path.exists(temp_raster):
                os.remove(temp_raster)
    
    try:
        # Get all dates
        dates = var_data.time.values
        total_days = len(dates)
        logger.info(f"Processing {total_days} days")
        
        # Process data in chunks
        chunk_size = 20  # Process 20 days at a time
        results = []
        
        for chunk_start in range(0, total_days, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_days)
            logger.info(f"Processing chunk {chunk_start//chunk_size + 1} of {(total_days + chunk_size - 1)//chunk_size}")
            
            # Get chunk data
            chunk_data = var_data.isel(time=slice(chunk_start, chunk_end))
            chunk_data = chunk_data.compute()  # Load chunk into memory
            
            # Process each day in the chunk
            for i in range(chunk_end - chunk_start):
                abs_idx = chunk_start + i
                if abs_idx < total_days:  # Guard against out-of-bounds
                    day_data = chunk_data.isel(time=i)
                    daily_values = process_day(day_data, abs_idx)
                    results.append((pd.Timestamp(dates[abs_idx]), daily_values))
        
        # Convert results to DataFrame
        df = pd.DataFrame({date: values for date, values in results}, index=counties.id_numeric)
        df.index.name = 'id_numeric'
        
        return df
        
    finally:
        # Cleanup temporary directory
        if os.path.exists(temp_dir):
            import shutil
            shutil.rmtree(temp_dir)

def calculate_annual_stats(daily_data, variable, stat_type):
    """Calculate annual statistics based on daily data."""
    if stat_type == 'mean':
        return daily_data.mean(axis=1)
    elif stat_type == 'sum':
        return daily_data.sum(axis=1)
    elif stat_type == 'threshold':
        if 'tasmax' in variable:
            # Count days above 90°F (32.22°C)
            return (daily_data > THRESHOLDS['tasmax_hot']).sum(axis=1)
        elif 'tasmin' in variable:
            # Count days below 32°F (0°C)
            return (daily_data < THRESHOLDS['tasmin_cold']).sum(axis=1)
        elif 'pr' in variable:
            # Count days with > 1 inch (25.4mm) precipitation
            return (daily_data > THRESHOLDS['pr_heavy']).sum(axis=1)
    return None

def main():
    # Define paths
    climate_data_path = "output/climate_means/conus_historical_climate_averages.nc"
    counties_path = "output/county_boundaries/contiguous_us_counties.parquet"
    output_dir = "output/county_climate_stats"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize Dask client for distributed computing
    logger.info("Initializing Dask distributed client")
    cluster = LocalCluster(n_workers=MAX_PROCESSES, memory_limit=MEMORY_LIMIT)
    client = Client(cluster)
    logger.info(f"Dask dashboard available at: {client.dashboard_link}")
    
    try:
        # Load and validate data
        climate_data = load_climate_data(climate_data_path)
        
        # Verify all required variables are present
        required_vars = ['tas_2012-2014', 'tasmax_2012-2014', 'tasmin_2012-2014', 'pr_2012-2014']
        missing_vars = [var for var in required_vars if var not in climate_data.data_vars]
        if missing_vars:
            raise ValueError(f"Missing required climate variables: {missing_vars}")
        
        counties = load_county_boundaries(counties_path)
        
        # Verify county data has required columns
        required_cols = ['GEOID', 'NAME', 'STUSPS', 'geometry']
        missing_cols = [col for col in required_cols if col not in counties.columns]
        if missing_cols:
            raise ValueError(f"Missing required county columns: {missing_cols}")
        
        # Define variables and their statistics
        variable_stats = {
            'tas_2012-2014': ('mean', 'mean_annual_temp'),
            'tasmax_2012-2014': ('threshold', 'days_above_90F'),
            'tasmin_2012-2014': ('threshold', 'days_below_32F'),
            'pr_2012-2014': [
                ('sum', 'total_annual_precip'),
                ('threshold', 'days_above_1inch_precip')
            ]
        }
        
        # Process each variable
        results = {}
        for var, stats in variable_stats.items():
            if not isinstance(stats, list):
                stats = [stats]
                
            for stat_type, col_name in stats:
                logger.info(f"Processing {var} for {stat_type} -> {col_name}")
                
                # Compute daily statistics
                daily_stats = compute_daily_zonal_stats(climate_data, counties, var, stat_type, output_dir)
                
                # Save daily data
                daily_file = os.path.join(output_dir, f"county_{var}_{stat_type}_daily.parquet")
                daily_stats.to_parquet(daily_file)
                logger.info(f"Saved daily statistics to {daily_file}")
                
                # Calculate annual statistic
                annual_stat = calculate_annual_stats(daily_stats, var, stat_type)
                results[col_name] = annual_stat
                logger.info(f"Calculated annual statistics for {col_name}")
        
        # Create final combined dataset
        logger.info("Creating combined annual statistics file")
        final_stats = counties[['id_numeric', 'GEOID', 'NAME', 'STUSPS', 'geometry']].copy()
        
        for col_name, data in results.items():
            final_stats[col_name] = data
            logger.info(f"Added {col_name} to final statistics")
        
        # Save results
        output_csv = os.path.join(output_dir, "county_climate_stats.csv")
        final_stats.drop(columns='geometry').to_csv(output_csv, index=False)
        logger.info(f"Saved CSV statistics to {output_csv}")
        
        output_gpkg = os.path.join(output_dir, "county_climate_stats.gpkg")
        final_stats.to_file(output_gpkg, driver="GPKG")
        logger.info(f"Saved GeoPackage to {output_gpkg}")
        
        # Print summary statistics
        logger.info("\nSummary of county climate statistics:")
        for col in ['mean_annual_temp', 'days_above_90F', 'days_below_32F', 
                    'total_annual_precip', 'days_above_1inch_precip']:
            stats = final_stats[col].describe()
            logger.info(f"\n{col}:")
            logger.info(f"  Mean: {stats['mean']:.2f}")
            logger.info(f"  Min: {stats['min']:.2f}")
            logger.info(f"  Max: {stats['max']:.2f}")
            logger.info(f"  Std: {stats['std']:.2f}")
        
        logger.info("Processing complete!")
        
    finally:
        # Clean up dask client and cluster
        client.close()
        cluster.close()

if __name__ == "__main__":
    main() 