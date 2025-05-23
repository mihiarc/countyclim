#!/usr/bin/env python3
"""
county_stats_partial.py

Partial run script to quickly generate precipitation data for validation.
Processes only a subset of counties to get results faster.
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
import time
import psutil
from tqdm import tqdm
import threading

warnings.filterwarnings('ignore', category=RuntimeWarning)

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Dask configuration for partial run
CHUNK_SIZE = {'time': 365}
MAX_PROCESSES = mp.cpu_count() - 1
MEMORY_LIMIT = '16GB'  # Reduced for partial run

# Define climate thresholds
THRESHOLDS = {
    'tasmax_hot': 32.22,    # 90°F in Celsius
    'tasmin_cold': 0.0,     # 32°F in Celsius
    'pr_heavy': 25.4        # 1 inch in mm
}

# Partial run configuration
SUBSET_SIZE = 100  # Process only first 100 counties for quick validation
TARGET_VARIABLES = ['pr_2012-2014']  # Focus on precipitation only

def load_climate_data(climate_data_path):
    """Load climate data from NetCDF file."""
    logger.info(f"Loading climate data from {climate_data_path}")
    ds = xr.open_dataset(climate_data_path, chunks=CHUNK_SIZE)
    
    logger.info(f"Climate variables: {list(ds.data_vars)}")
    logger.info(f"Dimensions: {ds.dims}")
    logger.info(f"Time range: {ds.time.values[0]} to {ds.time.values[-1]}")
    
    # Calculate data size
    total_size_gb = sum(var.nbytes for var in ds.data_vars.values()) / (1024**3)
    logger.info(f"Total data size: {total_size_gb:.2f} GB")
    
    return ds

def load_county_boundaries_subset(boundaries_path, subset_size=SUBSET_SIZE):
    """Load a subset of county boundaries for quick testing."""
    logger.info(f"Loading county boundaries from {boundaries_path}")
    counties = gpd.read_parquet(boundaries_path)
    
    # Take a subset for quick processing
    counties_subset = counties.head(subset_size).copy()
    
    logger.info(f"Loaded {len(counties_subset)} counties (subset of {len(counties)} total)")
    logger.info(f"CRS: {counties_subset.crs}")
    
    # Create id_numeric if needed
    if 'id_numeric' not in counties_subset.columns:
        logger.info("Creating id_numeric field")
        counties_subset['id_numeric'] = counties_subset.index + 1
    
    # Create STUSPS if needed
    if 'STUSPS' not in counties_subset.columns and 'STATEFP' in counties_subset.columns:
        logger.info("Creating STUSPS field from STATEFP")
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
        counties_subset['STUSPS'] = counties_subset['STATEFP'].map(state_fips_to_postal)
        logger.info("Added STUSPS column")
    
    return counties_subset

def compute_daily_zonal_stats_fast(climate_data, counties, variable, stat_type, output_dir):
    """Fast computation for partial runs - smaller batches, immediate processing."""
    
    logger.info(f"Computing daily zonal statistics for {variable} ({stat_type}) - PARTIAL RUN")
    
    var_data = climate_data[variable]
    
    # Get coordinate information
    lon_values = var_data.lon.values
    lat_values = var_data.lat.values
    
    # Calculate data size for this variable
    var_size_gb = var_data.nbytes / (1024**3)
    logger.info(f"Variable {variable} size: {var_size_gb:.2f} GB")
    
    transform = rasterio.transform.from_bounds(
        np.min(lon_values), 
        np.min(lat_values), 
        np.max(lon_values), 
        np.max(lat_values), 
        len(lon_values), 
        len(lat_values)
    )
    
    # Save counties to a temporary file
    temp_counties_file = os.path.join(output_dir, 'temp_counties_partial.gpkg')
    counties.to_file(temp_counties_file, driver="GPKG")
    
    def process_batch_of_days(batch_data_array, batch_dates, batch_start_idx, 
                             counties_file, transform_params, variable_name, stat_type):
        """Process a batch of days efficiently."""
        import geopandas as gpd
        import rasterio
        import numpy as np
        import pandas as pd
        from rasterstats import zonal_stats
        import tempfile
        import os
        import time
        
        batch_start_time = time.time()
        
        # Load counties from file
        counties_local = gpd.read_file(counties_file)
        
        batch_results = {}
        
        for i, (date, data_array) in enumerate(zip(batch_dates, batch_data_array)):
            # Use a unique temporary file
            with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp_file:
                temp_raster = tmp_file.name
            
            try:
                # Prepare data for writing
                data_to_write = np.flipud(data_array)
                
                # Write raster
                with rasterio.open(
                    temp_raster, 
                    'w', 
                    driver='GTiff', 
                    height=data_to_write.shape[0], 
                    width=data_to_write.shape[1], 
                    count=1, 
                    dtype=data_to_write.dtype, 
                    crs='+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs', 
                    transform=rasterio.transform.from_bounds(*transform_params, 
                                                           data_to_write.shape[1], 
                                                           data_to_write.shape[0])
                ) as dst:
                    dst.write(data_to_write, 1)
                
                # Compute zonal statistics
                if stat_type == 'threshold':
                    stats = zonal_stats(counties_local, temp_raster, stats=['mean'])
                    daily_values = [s['mean'] for s in stats]
                else:
                    stats = zonal_stats(counties_local, temp_raster, stats=[stat_type])
                    daily_values = [s[stat_type] for s in stats]
                
                batch_results[pd.Timestamp(date)] = daily_values
                
            finally:
                # Clean up temporary file
                if os.path.exists(temp_raster):
                    os.remove(temp_raster)
        
        batch_duration = time.time() - batch_start_time
        return batch_results, batch_duration, len(batch_dates)
    
    try:
        # Get all dates
        dates = var_data.time.values
        total_days = len(dates)
        logger.info(f"Processing {total_days} days for {variable}")
        
        # Use smaller batches for faster feedback
        batch_size = 15  # Smaller batches for partial run
        daily_results = {}
        
        # Transform parameters
        transform_params = (np.min(lon_values), np.min(lat_values), 
                          np.max(lon_values), np.max(lat_values))
        
        # Process batches
        futures = []
        total_batches = (total_days + batch_size - 1) // batch_size
        
        logger.info(f"Creating {total_batches} batches of ~{batch_size} days each")
        
        for batch_start in range(0, total_days, batch_size):
            batch_end = min(batch_start + batch_size, total_days)
            
            # Pre-compute batch data immediately for faster development feedback
            batch_slice = slice(batch_start, batch_end)
            logger.info(f"Loading batch {batch_start//batch_size + 1}/{total_batches}: days {batch_start} to {batch_end}")
            batch_data = var_data.isel(time=batch_slice).compute()  # Load data immediately
            batch_dates = dates[batch_start:batch_end]
            
            # Create a delayed task for the batch processing
            future = dask.delayed(process_batch_of_days)(
                batch_data.values,  # Now a small numpy array
                batch_dates,
                batch_start,
                temp_counties_file,
                transform_params,
                variable,
                stat_type
            )
            futures.append(future)
        
        # Compute all batches with progress tracking
        logger.info(f"Computing {len(futures)} batches in parallel...")
        
        start_compute_time = time.time()
        with tqdm(total=len(futures), desc=f"Processing {variable}", unit="batch") as pbar:
            results = dask.compute(*futures)
            pbar.update(len(futures))
        
        compute_duration = time.time() - start_compute_time
        
        # Process results
        total_processing_time = 0
        total_days_processed = 0
        
        for result in results:
            batch_results, batch_duration, days_in_batch = result
            daily_results.update(batch_results)
            total_processing_time += batch_duration
            total_days_processed += days_in_batch
        
        # Log performance metrics
        days_per_second = total_days_processed / compute_duration if compute_duration > 0 else 0
        
        logger.info(f"Batch processing completed:")
        logger.info(f"  Total compute time: {compute_duration:.2f} seconds")
        logger.info(f"  Processing rate: {days_per_second:.2f} days/second")
        
        # Convert results to DataFrame
        logger.info("Converting results to DataFrame...")
        df = pd.DataFrame(daily_results, index=counties.id_numeric)
        df.index.name = 'id_numeric'
        
        logger.info(f"Created DataFrame with shape: {df.shape}")
        
        return df
        
    finally:
        # Cleanup temporary counties file
        if os.path.exists(temp_counties_file):
            os.remove(temp_counties_file)

def calculate_annual_stats(daily_data, variable, stat_type):
    """Calculate annual statistics based on daily data."""
    if stat_type == 'mean':
        return daily_data.mean(axis=1)
    elif stat_type == 'sum':
        return daily_data.sum(axis=1)
    elif stat_type == 'threshold':
        if 'tasmax' in variable:
            return (daily_data > THRESHOLDS['tasmax_hot']).sum(axis=1)
        elif 'tasmin' in variable:
            return (daily_data < THRESHOLDS['tasmin_cold']).sum(axis=1)
        elif 'pr' in variable:
            return (daily_data > THRESHOLDS['pr_heavy']).sum(axis=1)
    return None

def main():
    start_time = time.time()
    
    # Define paths
    climate_data_path = "output/climate_means/conus_historical_climate_averages.nc"
    counties_path = "output/county_boundaries/contiguous_us_counties.parquet"
    output_dir = "output/county_climate_stats_partial"
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    logger.info("=== PARTIAL RUN FOR PRECIPITATION VALIDATION ===")
    logger.info(f"Processing subset of {SUBSET_SIZE} counties")
    logger.info(f"Target variables: {TARGET_VARIABLES}")
    
    # Initialize Dask client
    logger.info("Initializing Dask distributed client")
    cluster = LocalCluster(n_workers=MAX_PROCESSES, memory_limit=MEMORY_LIMIT)
    client = Client(cluster)
    logger.info(f"Dask dashboard available at: {client.dashboard_link}")
    
    try:
        # Load data
        climate_data = load_climate_data(climate_data_path)
        counties = load_county_boundaries_subset(counties_path, SUBSET_SIZE)
        
        # Define precipitation variables and their statistics
        variable_stats = {
            'pr_2012-2014': [
                ('sum', 'total_annual_precip'),
                ('threshold', 'days_above_1inch_precip')
            ]
        }
        
        # Process precipitation variables
        results = {}
        
        for var, stats in variable_stats.items():
            if var not in TARGET_VARIABLES:
                continue
                
            for stat_type, col_name in stats:
                logger.info(f"Processing {var} for {stat_type} -> {col_name}")
                
                # Compute daily statistics
                daily_stats = compute_daily_zonal_stats_fast(climate_data, counties, var, stat_type, output_dir)
                
                # Save daily data
                daily_file = os.path.join(output_dir, f"county_{var}_{stat_type}_daily.parquet")
                daily_stats.to_parquet(daily_file)
                file_size_mb = os.path.getsize(daily_file) / (1024**2)
                logger.info(f"Saved daily statistics to {daily_file} ({file_size_mb:.2f} MB)")
                
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
        output_csv = os.path.join(output_dir, "county_climate_stats_partial.csv")
        final_stats.drop(columns='geometry').to_csv(output_csv, index=False)
        logger.info(f"Saved CSV statistics to {output_csv}")
        
        output_gpkg = os.path.join(output_dir, "county_climate_stats_partial.gpkg")
        final_stats.to_file(output_gpkg, driver="GPKG")
        logger.info(f"Saved GeoPackage to {output_gpkg}")
        
        # Print summary statistics
        logger.info("\nSummary of precipitation statistics:")
        for col in ['total_annual_precip', 'days_above_1inch_precip']:
            if col in final_stats.columns:
                stats = final_stats[col].describe()
                logger.info(f"\n{col}:")
                logger.info(f"  Mean: {stats['mean']:.2f}")
                logger.info(f"  Min: {stats['min']:.2f}")
                logger.info(f"  Max: {stats['max']:.2f}")
                logger.info(f"  Std: {stats['std']:.2f}")
        
        total_time = time.time() - start_time
        logger.info(f"\nPartial run completed in {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        
    finally:
        # Clean up dask client and cluster
        client.close()
        cluster.close()

if __name__ == "__main__":
    main() 