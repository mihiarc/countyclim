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
from dask.distributed import Client, LocalCluster, as_completed
import time
import psutil
from tqdm import tqdm
import threading

warnings.filterwarnings('ignore', category=RuntimeWarning)

# Set up logging with more detailed formatting
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

class PerformanceMonitor:
    """Class to monitor performance metrics during processing."""
    
    def __init__(self):
        self.start_time = None
        self.stage_times = {}
        self.memory_usage = []
        self.dask_task_counts = []
        self.monitoring_active = False
        self.monitor_thread = None
        
    def start_monitoring(self, client=None):
        """Start performance monitoring."""
        self.start_time = time.time()
        self.monitoring_active = True
        self.client = client
        
        # Start background monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_resources, daemon=True)
        self.monitor_thread.start()
        
        logger.info("Performance monitoring started")
        
    def stop_monitoring(self):
        """Stop performance monitoring."""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1)
        
        total_time = time.time() - self.start_time if self.start_time else 0
        logger.info(f"Total processing time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        
    def _monitor_resources(self):
        """Background thread to monitor system resources."""
        while self.monitoring_active:
            # Monitor memory usage
            memory_info = psutil.virtual_memory()
            self.memory_usage.append({
                'timestamp': time.time(),
                'used_gb': memory_info.used / (1024**3),
                'percent': memory_info.percent,
                'available_gb': memory_info.available / (1024**3)
            })
            
            # Monitor Dask tasks if client is available
            if self.client:
                try:
                    scheduler_info = self.client.scheduler_info()
                    task_counts = {
                        'timestamp': time.time(),
                        'total_tasks': len(scheduler_info.get('tasks', {})),
                        'processing': sum(1 for task in scheduler_info.get('tasks', {}).values() 
                                        if task.get('state') == 'processing'),
                        'memory': sum(1 for task in scheduler_info.get('tasks', {}).values() 
                                    if task.get('state') == 'memory'),
                        'waiting': sum(1 for task in scheduler_info.get('tasks', {}).values() 
                                     if task.get('state') == 'waiting')
                    }
                    self.dask_task_counts.append(task_counts)
                except Exception as e:
                    # Silently continue if we can't get scheduler info
                    pass
            
            time.sleep(5)  # Monitor every 5 seconds
    
    def start_stage(self, stage_name):
        """Start timing a processing stage."""
        self.stage_times[stage_name] = {'start': time.time()}
        logger.info(f"Starting stage: {stage_name}")
        
    def end_stage(self, stage_name):
        """End timing a processing stage."""
        if stage_name in self.stage_times:
            duration = time.time() - self.stage_times[stage_name]['start']
            self.stage_times[stage_name]['duration'] = duration
            logger.info(f"Completed stage '{stage_name}' in {duration:.2f} seconds ({duration/60:.2f} minutes)")
            return duration
        return 0
    
    def get_summary(self):
        """Get a summary of performance metrics."""
        summary = {
            'total_time': time.time() - self.start_time if self.start_time else 0,
            'stage_times': {k: v.get('duration', 0) for k, v in self.stage_times.items()},
            'peak_memory_gb': max([m['used_gb'] for m in self.memory_usage]) if self.memory_usage else 0,
            'avg_memory_gb': np.mean([m['used_gb'] for m in self.memory_usage]) if self.memory_usage else 0,
            'max_concurrent_tasks': max([t['total_tasks'] for t in self.dask_task_counts]) if self.dask_task_counts else 0
        }
        return summary
    
    def print_summary(self):
        """Print a detailed performance summary."""
        summary = self.get_summary()
        
        logger.info("\n" + "="*60)
        logger.info("PERFORMANCE SUMMARY")
        logger.info("="*60)
        logger.info(f"Total execution time: {summary['total_time']:.2f} seconds ({summary['total_time']/60:.2f} minutes)")
        logger.info(f"Peak memory usage: {summary['peak_memory_gb']:.2f} GB")
        logger.info(f"Average memory usage: {summary['avg_memory_gb']:.2f} GB")
        logger.info(f"Max concurrent Dask tasks: {summary['max_concurrent_tasks']}")
        
        logger.info("\nStage breakdown:")
        for stage, duration in summary['stage_times'].items():
            percentage = (duration / summary['total_time']) * 100 if summary['total_time'] > 0 else 0
            logger.info(f"  {stage}: {duration:.2f}s ({percentage:.1f}%)")
        
        logger.info("="*60)

# Global performance monitor
perf_monitor = PerformanceMonitor()

def load_climate_data(climate_data_path):
    """
    Load climate data from NetCDF file.
    
    Args:
        climate_data_path (str): Path to the climate data NetCDF file
        
    Returns:
        xarray.Dataset: Climate data
    """
    perf_monitor.start_stage("load_climate_data")
    
    logger.info(f"Loading climate data from {climate_data_path}")
    ds = xr.open_dataset(climate_data_path, chunks=CHUNK_SIZE)
    
    logger.info(f"Climate variables: {list(ds.data_vars)}")
    logger.info(f"Dimensions: {ds.dims}")
    logger.info(f"Time range: {ds.time.values[0]} to {ds.time.values[-1]}")
    logger.info(f"Chunk sizes: {ds.chunks}")
    
    # Calculate data size
    total_size_gb = sum(var.nbytes for var in ds.data_vars.values()) / (1024**3)
    logger.info(f"Total data size: {total_size_gb:.2f} GB")
    
    perf_monitor.end_stage("load_climate_data")
    return ds

def load_county_boundaries(boundaries_path):
    """
    Load county boundaries from parquet file.
    
    Args:
        boundaries_path (str): Path to the county boundaries parquet file
        
    Returns:
        geopandas.GeoDataFrame: County boundaries
    """
    perf_monitor.start_stage("load_county_boundaries")
    
    logger.info(f"Loading county boundaries from {boundaries_path}")
    counties = gpd.read_parquet(boundaries_path)
    
    logger.info(f"Loaded {len(counties)} county boundaries")
    logger.info(f"CRS: {counties.crs}")
    
    # Calculate memory usage of counties data
    counties_size_mb = counties.memory_usage(deep=True).sum() / (1024**2)
    logger.info(f"County boundaries memory usage: {counties_size_mb:.2f} MB")
    
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
    
    perf_monitor.end_stage("load_county_boundaries")
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
    stage_name = f"compute_daily_zonal_stats_{variable}_{stat_type}"
    perf_monitor.start_stage(stage_name)
    
    logger.info(f"Computing daily zonal statistics for {variable} ({stat_type})")
    
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
    
    # Save counties to a temporary file to avoid embedding in computation graph
    temp_counties_file = os.path.join(output_dir, 'temp_counties.gpkg')
    counties.to_file(temp_counties_file, driver="GPKG")
    
    def process_batch_of_days(batch_data_array, batch_dates, batch_start_idx, 
                             counties_file, transform_params, variable_name, stat_type):
        """Process a batch of days efficiently without file I/O conflicts."""
        import geopandas as gpd
        import rasterio
        import numpy as np
        import pandas as pd
        from rasterstats import zonal_stats
        import tempfile
        import os
        import time
        
        batch_start_time = time.time()
        
        # Load counties from file (not embedded in graph)
        counties_local = gpd.read_file(counties_file)
        
        batch_results = {}
        
        for i, (date, data_array) in enumerate(zip(batch_dates, batch_data_array)):
            # Use a unique temporary file for each process/thread
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
        
        # Process in larger batches to reduce overhead
        batch_size = 30  # Larger batches for efficiency
        daily_results = {}
        
        # Transform parameters for the batch function
        transform_params = (np.min(lon_values), np.min(lat_values), 
                          np.max(lon_values), np.max(lat_values))
        
        # Process batches
        futures = []
        total_batches = (total_days + batch_size - 1) // batch_size
        
        logger.info(f"Creating {total_batches} batches of ~{batch_size} days each")
        
        for batch_start in range(0, total_days, batch_size):
            batch_end = min(batch_start + batch_size, total_days)
            
            # Get batch data as a delayed object (don't compute yet)
            batch_slice = slice(batch_start, batch_end)
            batch_data = var_data.isel(time=batch_slice)
            batch_dates = dates[batch_start:batch_end]
            
            # Create a delayed task for the entire batch
            future = dask.delayed(process_batch_of_days)(
                batch_data.values,  # This will be computed lazily
                batch_dates,
                batch_start,
                temp_counties_file,  # File path, not the object itself
                transform_params,
                variable,
                stat_type
            )
            futures.append(future)
        
        # Compute all batches with progress tracking
        logger.info(f"Computing {len(futures)} batches in parallel...")
        
        # Use as_completed for progress tracking
        completed_batches = 0
        total_processing_time = 0
        total_days_processed = 0
        
        # Create progress bar
        with tqdm(total=len(futures), desc=f"Processing {variable}", unit="batch") as pbar:
            # Submit all futures and track completion
            batch_results_list = []
            
            # For now, compute all at once but we could use as_completed for real-time progress
            start_compute_time = time.time()
            results = dask.compute(*futures)
            compute_duration = time.time() - start_compute_time
            
            # Process results and update progress
            for result in results:
                batch_results, batch_duration, days_in_batch = result
                batch_results_list.append(batch_results)
                total_processing_time += batch_duration
                total_days_processed += days_in_batch
                completed_batches += 1
                pbar.update(1)
        
        # Log performance metrics
        avg_batch_time = total_processing_time / completed_batches if completed_batches > 0 else 0
        days_per_second = total_days_processed / compute_duration if compute_duration > 0 else 0
        
        logger.info(f"Batch processing completed:")
        logger.info(f"  Total compute time: {compute_duration:.2f} seconds")
        logger.info(f"  Average batch processing time: {avg_batch_time:.2f} seconds")
        logger.info(f"  Processing rate: {days_per_second:.2f} days/second")
        logger.info(f"  Parallel efficiency: {(total_processing_time/compute_duration):.2f}x")
        
        # Combine results from all batches
        logger.info("Combining results from all batches...")
        for batch_results in batch_results_list:
            daily_results.update(batch_results)
        
        # Convert results to DataFrame
        logger.info("Converting results to DataFrame...")
        df = pd.DataFrame(daily_results, index=counties.id_numeric)
        df.index.name = 'id_numeric'
        
        logger.info(f"Created DataFrame with shape: {df.shape}")
        
        perf_monitor.end_stage(stage_name)
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
    perf_monitor.start_stage("dask_setup")
    
    cluster = LocalCluster(n_workers=MAX_PROCESSES, memory_limit=MEMORY_LIMIT)
    client = Client(cluster)
    logger.info(f"Dask dashboard available at: {client.dashboard_link}")
    logger.info(f"Dask cluster: {MAX_PROCESSES} workers, {MEMORY_LIMIT} memory limit per worker")
    
    # Start performance monitoring
    perf_monitor.start_monitoring(client)
    perf_monitor.end_stage("dask_setup")
    
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
        
        # Calculate total processing workload
        total_computations = sum(len(stats) if isinstance(stats, list) else 1 
                               for stats in variable_stats.values())
        logger.info(f"Total computations to perform: {total_computations}")
        
        # Process each variable
        results = {}
        computation_count = 0
        
        for var, stats in variable_stats.items():
            if not isinstance(stats, list):
                stats = [stats]
                
            for stat_type, col_name in stats:
                computation_count += 1
                logger.info(f"Processing computation {computation_count}/{total_computations}: {var} for {stat_type} -> {col_name}")
                
                # Compute daily statistics
                daily_stats = compute_daily_zonal_stats(climate_data, counties, var, stat_type, output_dir)
                
                # Save daily data
                perf_monitor.start_stage(f"save_daily_{var}_{stat_type}")
                daily_file = os.path.join(output_dir, f"county_{var}_{stat_type}_daily.parquet")
                daily_stats.to_parquet(daily_file)
                file_size_mb = os.path.getsize(daily_file) / (1024**2)
                logger.info(f"Saved daily statistics to {daily_file} ({file_size_mb:.2f} MB)")
                perf_monitor.end_stage(f"save_daily_{var}_{stat_type}")
                
                # Calculate annual statistic
                perf_monitor.start_stage(f"calc_annual_{var}_{stat_type}")
                annual_stat = calculate_annual_stats(daily_stats, var, stat_type)
                results[col_name] = annual_stat
                logger.info(f"Calculated annual statistics for {col_name}")
                perf_monitor.end_stage(f"calc_annual_{var}_{stat_type}")
        
        # Create final combined dataset
        perf_monitor.start_stage("create_final_dataset")
        logger.info("Creating combined annual statistics file")
        final_stats = counties[['id_numeric', 'GEOID', 'NAME', 'STUSPS', 'geometry']].copy()
        
        for col_name, data in results.items():
            final_stats[col_name] = data
            logger.info(f"Added {col_name} to final statistics")
        
        # Save results
        output_csv = os.path.join(output_dir, "county_climate_stats.csv")
        final_stats.drop(columns='geometry').to_csv(output_csv, index=False)
        csv_size_mb = os.path.getsize(output_csv) / (1024**2)
        logger.info(f"Saved CSV statistics to {output_csv} ({csv_size_mb:.2f} MB)")
        
        output_gpkg = os.path.join(output_dir, "county_climate_stats.gpkg")
        final_stats.to_file(output_gpkg, driver="GPKG")
        gpkg_size_mb = os.path.getsize(output_gpkg) / (1024**2)
        logger.info(f"Saved GeoPackage to {output_gpkg} ({gpkg_size_mb:.2f} MB)")
        
        perf_monitor.end_stage("create_final_dataset")
        
        # Print summary statistics
        perf_monitor.start_stage("summary_stats")
        logger.info("\nSummary of county climate statistics:")
        for col in ['mean_annual_temp', 'days_above_90F', 'days_below_32F', 
                    'total_annual_precip', 'days_above_1inch_precip']:
            if col in final_stats.columns:
                stats = final_stats[col].describe()
                logger.info(f"\n{col}:")
                logger.info(f"  Mean: {stats['mean']:.2f}")
                logger.info(f"  Min: {stats['min']:.2f}")
                logger.info(f"  Max: {stats['max']:.2f}")
                logger.info(f"  Std: {stats['std']:.2f}")
        
        perf_monitor.end_stage("summary_stats")
        
        logger.info("Processing complete!")
        
        # Print comprehensive performance summary
        perf_monitor.stop_monitoring()
        perf_monitor.print_summary()
        
        # Save performance metrics to file
        perf_summary = perf_monitor.get_summary()
        perf_file = os.path.join(output_dir, "performance_metrics.json")
        import json
        with open(perf_file, 'w') as f:
            # Convert numpy types to native Python types for JSON serialization
            json_summary = {}
            for key, value in perf_summary.items():
                if isinstance(value, dict):
                    json_summary[key] = {k: float(v) if hasattr(v, 'item') else v for k, v in value.items()}
                else:
                    json_summary[key] = float(value) if hasattr(value, 'item') else value
            json.dump(json_summary, f, indent=2)
        logger.info(f"Saved performance metrics to {perf_file}")
        
    finally:
        # Clean up dask client and cluster
        client.close()
        cluster.close()

if __name__ == "__main__":
    main() 