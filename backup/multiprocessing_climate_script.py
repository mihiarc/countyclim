#!/usr/bin/env python3
"""
county_climate_stats_multiprocessing.py

High-performance multiprocessing implementation for calculating county-level climate statistics.
Optimized for systems with high CPU count and abundant memory.
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
import time
import psutil
from tqdm import tqdm
import multiprocessing as mp
from multiprocessing import Pool, shared_memory, Manager
import pickle
from pathlib import Path
from functools import partial
import gc

warnings.filterwarnings('ignore', category=RuntimeWarning)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('county_stats_multiprocessing.log')
    ]
)
logger = logging.getLogger(__name__)

class PerformanceMonitor:
    """Enhanced performance monitoring class with memory tracking."""
    
    def __init__(self):
        self.start_time = None
        self.stage_times = {}
        
    def start_monitoring(self):
        """Start performance monitoring."""
        self.start_time = time.time()
        self._log_memory_usage("Monitoring started")
        logger.info("Performance monitoring started")
        
    def stop_monitoring(self):
        """Stop performance monitoring."""
        total_time = time.time() - self.start_time if self.start_time else 0
        self._log_memory_usage("Monitoring stopped")
        logger.info(f"Total processing time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        
    def start_stage(self, stage_name):
        """Start timing a processing stage."""
        self.stage_times[stage_name] = {'start': time.time()}
        self._log_memory_usage(f"Starting {stage_name}")
        logger.info(f"Starting stage: {stage_name}")
        
    def end_stage(self, stage_name):
        """End timing a processing stage."""
        if stage_name in self.stage_times:
            duration = time.time() - self.stage_times[stage_name]['start']
            self.stage_times[stage_name]['duration'] = duration
            self._log_memory_usage(f"Completed {stage_name}")
            logger.info(f"Completed stage '{stage_name}' in {duration:.2f} seconds ({duration/60:.2f} minutes)")
            return duration
        return 0
    
    def _log_memory_usage(self, context):
        """Log current memory usage."""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            logger.info(f"{context} - Memory usage: {memory_mb:.1f} MB")
        except Exception:
            pass  # Don't fail if memory monitoring fails
    
    def print_summary(self):
        """Print a detailed performance summary."""
        total_time = time.time() - self.start_time if self.start_time else 0
        
        logger.info("\n" + "="*60)
        logger.info("PERFORMANCE SUMMARY")
        logger.info("="*60)
        logger.info(f"Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        
        logger.info("\nStage breakdown:")
        for stage, data in self.stage_times.items():
            duration = data.get('duration', 0)
            percentage = (duration / total_time) * 100 if total_time > 0 else 0
            logger.info(f"  {stage}: {duration:.2f}s ({percentage:.1f}%)")
        
        logger.info("="*60)

def validate_climate_data(ds, expected_vars):
    """Validate climate data structure."""
    missing_vars = set(expected_vars) - set(ds.data_vars)
    if missing_vars:
        raise ValueError(f"Missing variables: {missing_vars}")
    
    if 'time' not in ds.dims:
        raise ValueError("No time dimension found")
    
    logger.info("Climate data validation passed")
    return True

def load_climate_data(climate_data_path):
    """Load climate data from NetCDF file."""
    logger.info(f"Loading climate data from {climate_data_path}")
    
    # Load without chunking for multiprocessing approach
    # We'll handle parallelism at the processing level
    ds = xr.open_dataset(climate_data_path)
    
    logger.info(f"Climate variables: {list(ds.data_vars)}")
    logger.info(f"Dimensions: {ds.dims}")
    logger.info(f"Time range: {ds.time.values[0]} to {ds.time.values[-1]}")
    
    return ds

def load_county_boundaries(boundaries_path):
    """Load county boundaries from parquet file."""
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

def setup_shared_memory_arrays(climate_data, variable_name):
    """Set up shared memory arrays for efficient multiprocessing."""
    logger.info("Setting up shared memory arrays for multiprocessing")
    
    data_array = climate_data[variable_name]
    
    # Handle different dimension orders
    if data_array.dims == ('lat', 'lon', 'time'):
        data_values = data_array.values
        shape = data_values.shape
        dims_order = ('lat', 'lon', 'time')
    elif data_array.dims == ('time', 'lat', 'lon'):
        data_values = data_array.values
        shape = data_values.shape
        dims_order = ('time', 'lat', 'lon')
    else:
        raise ValueError(f"Unexpected dimensions: {data_array.dims}")
    
    # Calculate size in bytes
    size_bytes = data_values.nbytes
    logger.info(f"Creating shared memory array of size {size_bytes / 1024 / 1024:.1f} MB")
    
    # Create shared memory
    shm = shared_memory.SharedMemory(create=True, size=size_bytes)
    
    # Create numpy array backed by shared memory
    shared_array = np.ndarray(shape, dtype=data_values.dtype, buffer=shm.buf)
    
    # Copy data to shared memory
    shared_array[:] = data_values[:]
    
    # Get coordinate arrays
    lat_coords = data_array.lat.values
    lon_coords = data_array.lon.values
    time_coords = data_array.time.values
    
    logger.info("Shared memory setup complete")
    
    return shm, shared_array, shape, dims_order, lat_coords, lon_coords, time_coords

def process_time_slice_worker_mp(args):
    """
    Process a single time slice in a multiprocessing worker.
    
    Args:
        args: Tuple containing (time_idx, shm_name, shape, dtype, dims_order, 
                               counties_geom_serialized, transform_params, stat_type, time_values)
    """
    try:
        (time_idx, shm_name, shape, dtype, dims_order, 
         counties_geom_serialized, transform_params, stat_type, time_values) = args
        
        # Connect to shared memory
        shm = shared_memory.SharedMemory(name=shm_name)
        shared_array = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
        
        # Extract the time slice based on dimension order
        if dims_order == ('lat', 'lon', 'time'):
            data_slice = shared_array[:, :, time_idx]
        elif dims_order == ('time', 'lat', 'lon'):
            data_slice = shared_array[time_idx, :, :]
        else:
            raise ValueError(f"Unexpected dimensions order: {dims_order}")
        
        # Deserialize counties geometry
        counties_geom = pickle.loads(counties_geom_serialized)
        
        # Replace NaN with nodata value
        data_slice = np.where(np.isnan(data_slice), -9999, data_slice)
        
        # Calculate zonal statistics
        stats = zonal_stats(
            counties_geom,
            data_slice,
            affine=transform_params,
            stats=[stat_type],
            nodata=-9999
        )
        
        # Extract results
        results = [s[stat_type] if s[stat_type] is not None else 0.0 for s in stats]
        
        # Get actual date from time coordinates
        actual_date = pd.Timestamp(time_values[time_idx])
        
        # Clean up
        shm.close()
        
        return time_idx, results, actual_date
        
    except Exception as e:
        # Clean up on error
        try:
            shm.close()
        except:
            pass
        return time_idx, None, str(e)

def calculate_county_statistics_mp(climate_data, counties, variable_name, stat_type, n_processes=None):
    """Calculate county statistics using multiprocessing."""
    logger.info(f"Calculating {stat_type} statistics for {variable_name} using multiprocessing")
    
    if n_processes is None:
        # Use most available cores, leaving a few for system
        n_processes = max(1, min(56, psutil.cpu_count() - 4))
    
    logger.info(f"Using {n_processes} processes for parallel computation")
    
    try:
        # Validate data first
        validate_climate_data(climate_data, [variable_name])
        
        # Get transform parameters
        transform_params = rasterio.transform.from_origin(
            climate_data.lon.min().values,
            climate_data.lat.max().values,
            np.abs(climate_data.lon[1].values - climate_data.lon[0].values),
            np.abs(climate_data.lat[1].values - climate_data.lat[0].values)
        )
        
        # Set up shared memory for the climate data
        shm, shared_array, shape, dims_order, lat_coords, lon_coords, time_coords = setup_shared_memory_arrays(
            climate_data, variable_name
        )
        
        # Get data info
        data_array = climate_data[variable_name]
        if 'time' in data_array.dims:
            n_times = data_array.sizes['time']
        else:
            raise ValueError(f"No time dimension found in {variable_name}")
            
        logger.info(f"Processing {n_times} time steps")
        logger.info(f"Data array dims: {data_array.dims}")
        logger.info(f"Data array shape: {data_array.shape}")
        logger.info(f"Time range: {pd.Timestamp(time_coords[0])} to {pd.Timestamp(time_coords[-1])}")
        
        # Serialize county geometries
        counties_geom_serialized = pickle.dumps(counties.geometry.values)
        logger.info(f"Serialized {len(counties)} county geometries ({len(counties_geom_serialized)/1024/1024:.1f} MB)")
        
        # Prepare arguments for worker processes
        worker_args = []
        for time_idx in range(n_times):
            args = (
                time_idx,
                shm.name,
                shape,
                shared_array.dtype,
                dims_order,
                counties_geom_serialized,
                transform_params,
                stat_type,
                time_coords
            )
            worker_args.append(args)
        
        logger.info(f"Created {len(worker_args)} tasks for multiprocessing")
        
        # Process in batches to manage memory and provide progress updates
        batch_size = min(n_processes * 4, len(worker_args))  # Process 4x processes worth at a time
        results = {}
        failed_tasks = []
        
        total_batches = (len(worker_args) - 1) // batch_size + 1
        logger.info(f"Processing in {total_batches} batches of {batch_size} tasks each")
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(worker_args))
            batch_args = worker_args[start_idx:end_idx]
            
            logger.info(f"⚡ Processing batch {batch_num + 1}/{total_batches} (tasks {start_idx}-{end_idx-1})")
            
            # Process batch with multiprocessing
            with Pool(processes=n_processes) as pool:
                batch_results = pool.map(process_time_slice_worker_mp, batch_args)
            
            # Process results
            successful = 0
            for result in batch_results:
                if len(result) == 3:  # Normal success case
                    time_idx, time_results, actual_date = result
                    if time_results is not None:
                        results[actual_date] = time_results
                        successful += 1
                    else:
                        failed_tasks.append(time_idx)
                        logger.warning(f"Failed to process time index {time_idx}")
                else:  # Error case
                    time_idx, error_msg = result[:2]
                    failed_tasks.append(time_idx)
                    logger.error(f"Error processing time index {time_idx}: {error_msg}")
            
            logger.info(f"✅ Batch {batch_num + 1} completed - {successful}/{len(batch_results)} successful")
            
            # Force garbage collection between batches
            gc.collect()
        
        # Report final results
        logger.info(f"🎉 Successfully processed {len(results)} time steps")
        if failed_tasks:
            logger.warning(f"⚠️  Failed to process {len(failed_tasks)} time steps: {failed_tasks[:10]}{'...' if len(failed_tasks) > 10 else ''}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in calculate_county_statistics_mp: {str(e)}")
        raise
    finally:
        # Clean up shared memory
        try:
            shm.close()
            shm.unlink()  # Delete the shared memory
            logger.info("Cleaned up shared memory")
        except Exception as e:
            logger.warning(f"Error cleaning up shared memory: {e}")

def main():
    """Main execution function."""
    # Define paths using pathlib for better path handling
    climate_data_path = Path("output/climate_means/conus_historical_climate_means_2012-2014.nc")
    counties_path = Path("output/county_boundaries/contiguous_us_counties.parquet")
    output_dir = Path("output/county_climate_stats")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Log system information
    total_cpu_cores = psutil.cpu_count(logical=True)
    total_memory_gb = psutil.virtual_memory().total / (1024**3)
    logger.info(f"System specs: {total_cpu_cores} CPU cores, {total_memory_gb:.1f}GB total RAM")
    
    # Initialize performance monitoring
    perf_monitor = PerformanceMonitor()
    perf_monitor.start_monitoring()
    
    try:
        # Load data
        perf_monitor.start_stage("load_data")
        logger.info("Loading input data...")
        
        climate_data = load_climate_data(climate_data_path)
        if climate_data is None:
            raise Exception("Failed to load climate data")
        
        counties = load_county_boundaries(counties_path)
        if counties is None or len(counties) == 0:
            raise Exception("Failed to load county boundaries")
            
        logger.info(f"Successfully loaded {len(counties)} counties")
        perf_monitor.end_stage("load_data")
        
        # Process precipitation data
        variables_to_process = {
            'pr_2014': {
                'stat_type': 'sum',
                'description': 'Total precipitation'
            }
        }
        
        # Validate that variables exist
        available_vars = list(climate_data.data_vars)
        for var_name in variables_to_process:
            if var_name not in available_vars:
                logger.warning(f"Variable {var_name} not found in climate data. Available: {available_vars}")
        
        # Process each variable
        all_results = {}
        for var_name, config in variables_to_process.items():
            if var_name in available_vars:
                perf_monitor.start_stage(f"process_{var_name}")
                logger.info(f"Processing {var_name}")
                
                results = calculate_county_statistics_mp(
                    climate_data,
                    counties,
                    var_name,
                    config['stat_type'],
                    n_processes=56  # Use all 56 available workers
                )
                all_results[var_name] = results
                perf_monitor.end_stage(f"process_{var_name}")
            else:
                logger.error(f"Skipping {var_name} - not found in climate data")
        
        if not all_results:
            raise Exception("No variables were successfully processed")
        
        # Convert results to DataFrame
        perf_monitor.start_stage("create_dataframe")
        logger.info("Converting results to DataFrame...")
        
        # Create a DataFrame with proper structure
        first_var = list(all_results.keys())[0]
        dates = sorted(all_results[first_var].keys())
        n_counties = len(counties)
        
        logger.info(f"Creating DataFrame for {len(dates)} dates and {n_counties} counties")
        
        # Initialize DataFrame
        df_data = {}
        for var_name in all_results:
            var_data = []
            for date in dates:
                if date in all_results[var_name]:
                    var_data.extend(all_results[var_name][date])
                else:
                    # Fill missing dates with NaN
                    var_data.extend([np.nan] * n_counties)
            df_data[var_name] = var_data
        
        # Create multi-index for counties and dates
        county_ids = list(range(n_counties)) * len(dates)
        date_list = []
        for date in dates:
            date_list.extend([date] * n_counties)
        
        output_df = pd.DataFrame(df_data)
        output_df['county_id'] = county_ids
        output_df['date'] = date_list
        
        # Reorder columns
        output_df = output_df[['county_id', 'date'] + list(all_results.keys())]
        perf_monitor.end_stage("create_dataframe")
        
        # Save results
        perf_monitor.start_stage("save_results")
        output_path = output_dir / 'county_climate_stats_multiprocessing.csv'
        output_df.to_csv(output_path, index=False)
        
        # Verify the file was created and log summary stats
        if output_path.exists():
            file_size_mb = output_path.stat().st_size / 1024 / 1024
            logger.info(f"Results successfully saved to {output_path}")
            logger.info(f"Output DataFrame shape: {output_df.shape}")
            logger.info(f"File size: {file_size_mb:.1f} MB")
            
            # Log some basic statistics
            logger.info("\nData summary:")
            for var in all_results.keys():
                if var in output_df.columns:
                    var_stats = output_df[var].describe()
                    logger.info(f"{var}: mean={var_stats['mean']:.2f}, std={var_stats['std']:.2f}, min={var_stats['min']:.2f}, max={var_stats['max']:.2f}")
        else:
            raise Exception(f"Failed to save results to {output_path}")
        
        perf_monitor.end_stage("save_results")
        
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}", exc_info=True)
        raise
    finally:
        perf_monitor.stop_monitoring()
        perf_monitor.print_summary()
        
if __name__ == '__main__':
    # Set multiprocessing start method to 'spawn' for better memory isolation
    mp.set_start_method('spawn', force=True)
    main()