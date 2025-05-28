#!/usr/bin/env python3
"""
county_climate_stats_dask_simple.py

Simple Dask-based implementation for calculating county-level climate statistics.
This version uses Dask workers with dashboard monitoring for distributed processing.
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
import dask
from dask.distributed import Client
from dask import delayed
import pickle
from pathlib import Path

# Configure Dask for high-performance systems
dask.config.set({
    'distributed.worker.memory.target': 0.8,  # Use 80% of worker memory before spilling
    'distributed.worker.memory.spill': 0.9,   # Spill at 90% memory usage
    'distributed.worker.memory.pause': 0.95,  # Pause at 95% memory usage
    'distributed.comm.timeouts.connect': '60s',  # Longer timeouts for large clusters
    'distributed.comm.timeouts.tcp': '60s',
    'distributed.scheduler.bandwidth': 1000000000,  # 1GB/s bandwidth assumption
    'array.chunk-size': '256MB',  # Larger chunks for high-memory systems
    'array.slicing.split_large_chunks': True
})

warnings.filterwarnings('ignore', category=RuntimeWarning)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('county_stats_dask_simple.log')
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
    """Load climate data from NetCDF file with optimized chunking for high-performance systems."""
    logger.info(f"Loading climate data from {climate_data_path}")
    
    # Optimize chunks for high-memory, high-CPU systems
    # Larger chunks reduce task overhead and improve throughput
    ds = xr.open_dataset(
        climate_data_path, 
        chunks={
            'time': 'auto',  # Let xarray decide optimal time chunking
            'lat': 200,      # Larger spatial chunks for better memory utilization
            'lon': 200       # Balance between memory usage and parallelism
        }
    )
    
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

def calculate_optimal_batch_size(n_tasks, n_workers):
    """Calculate optimal batch size based on number of workers."""
    # For high-performance systems, use larger batches to reduce overhead
    if n_workers >= 32:
        return max(n_workers, min(n_workers * 4, n_tasks // 4))
    else:
        return max(1, min(20, n_tasks // (n_workers * 2)))

def setup_dask_client(n_workers=None, threads_per_worker=1, memory_limit='1.5GB'):
    """Set up Dask client with dashboard monitoring optimized for high-performance systems."""
    total_cpu_cores = psutil.cpu_count(logical=True)
    total_memory_gb = psutil.virtual_memory().total / (1024**3)
    
    if n_workers is None:
        # For high-performance systems, use more workers with single threads
        # This reduces GIL contention and improves I/O parallelism
        if total_cpu_cores >= 32:
            n_workers = min(56, total_cpu_cores)  # Use up to 56 workers as available
            threads_per_worker = 1  # Single thread per worker for better I/O parallelism
        else:
            n_workers = max(1, min(16, total_cpu_cores // 2))
            threads_per_worker = 2
    
    # Calculate memory per worker based on available RAM
    if memory_limit == '1.5GB':  # Default case, calculate optimal
        # Reserve 20GB for system and main process, distribute rest among workers
        available_memory_gb = max(10, total_memory_gb - 20)
        memory_per_worker_gb = available_memory_gb / n_workers
        # Cap at 3GB per worker to avoid excessive memory usage per task
        memory_per_worker_gb = min(3.0, max(1.0, memory_per_worker_gb))
        memory_limit = f"{memory_per_worker_gb:.1f}GB"
    
    logger.info(f"System specs: {total_cpu_cores} CPU cores, {total_memory_gb:.1f}GB total RAM")
    logger.info(f"Setting up Dask client with {n_workers} workers, {threads_per_worker} threads per worker")
    logger.info(f"Memory limit per worker: {memory_limit}")
    
    # Create client with local cluster and dashboard
    client = Client(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        dashboard_address=':8787',
        silence_logs=False,
        # Optimize for high-performance processing
        processes=True,  # Use processes instead of threads for better parallelism
        # Configure connection timeouts for large clusters
        timeout='60s',
        # Enable spilling to disk if needed (though with 100GB RAM, unlikely)
        local_directory='/tmp/dask-worker-space'
    )
    
    logger.info(f"Dask dashboard available at: {client.dashboard_link}")
    logger.info("🚀 Open the dashboard in your browser to monitor progress!")
    
    # Print cluster info
    cluster_info = client.scheduler_info()
    logger.info(f"Cluster workers: {len(cluster_info['workers'])}")
    total_cores = sum(w['nthreads'] for w in cluster_info['workers'].values())
    total_memory_gb = sum(w['memory_limit'] for w in cluster_info['workers'].values()) / (1024**3)
    logger.info(f"Total cores: {total_cores}")
    logger.info(f"Total memory: {total_memory_gb:.2f} GB")
    
    return client

def process_time_slice_worker(climate_data_path, variable_name, time_idx, counties_geom_serialized, transform_params, stat_type, time_values):
    """
    Process a single time slice in a Dask worker.
    
    Args:
        climate_data_path: Path to NetCDF file
        variable_name: Name of variable to process
        time_idx: Time index to process
        counties_geom_serialized: Pickled county geometries
        transform_params: Rasterio transform parameters
        stat_type: Type of statistic to calculate
        time_values: Array of time coordinate values
    """
    try:
        # Import required modules in worker
        import xarray as xr
        import numpy as np
        import pickle
        from rasterstats import zonal_stats
        import pandas as pd
        
        # Load data in worker with minimal memory footprint
        with xr.open_dataset(climate_data_path) as ds:
            data_var = ds[variable_name]
            
            # Handle different dimension orders
            if data_var.dims == ('lat', 'lon', 'time'):
                data_slice = data_var.isel(time=time_idx).values
            elif data_var.dims == ('time', 'lat', 'lon'):
                data_slice = data_var.isel(time=time_idx).values
            else:
                raise ValueError(f"Unexpected dimensions: {data_var.dims}")
        
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
        
        return time_idx, results, actual_date
        
    except Exception as e:
        # Return error info for debugging
        return time_idx, None, str(e)

def calculate_county_statistics_dask(climate_data, counties, variable_name, stat_type, client):
    """Calculate county statistics using Dask workers with dashboard monitoring."""
    logger.info(f"Calculating {stat_type} statistics for {variable_name} using Dask workers")
    logger.info("📊 Check the Dask dashboard for real-time progress monitoring!")
    
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
        
        # Get data info - handle different dimension orders
        data_array = climate_data[variable_name]
        if 'time' in data_array.dims:
            n_times = data_array.sizes['time']
            time_values = data_array.time.values
        else:
            raise ValueError(f"No time dimension found in {variable_name}")
            
        logger.info(f"Processing {n_times} time steps")
        logger.info(f"Data array dims: {data_array.dims}")
        logger.info(f"Data array shape: {data_array.shape}")
        logger.info(f"Time range: {pd.Timestamp(time_values[0])} to {pd.Timestamp(time_values[-1])}")
        
        # Serialize county geometries with chunking for large datasets
        counties_geom_serialized = pickle.dumps(counties.geometry.values)
        logger.info(f"Serialized {len(counties)} county geometries ({len(counties_geom_serialized)/1024/1024:.1f} MB)")
        
        # Get the actual path for workers
        climate_data_path = climate_data.encoding.get('source')
        if not climate_data_path:
            # If source not available, we need to save and reload (fallback)
            temp_path = "temp_climate_data.nc"
            climate_data.to_netcdf(temp_path)
            climate_data_path = temp_path
            logger.warning(f"Using temporary file: {temp_path}")
        
        # Create delayed tasks
        delayed_tasks = []
        for time_idx in range(n_times):
            task = delayed(process_time_slice_worker)(
                climate_data_path, 
                variable_name, 
                time_idx, 
                counties_geom_serialized, 
                transform_params, 
                stat_type,
                time_values
            )
            delayed_tasks.append(task)
        
        logger.info(f"Created {len(delayed_tasks)} delayed tasks")
        logger.info("🔍 Tasks are now visible in the Dask dashboard - check the Graph and Tasks tabs!")
        
        # Calculate optimal batch size for high-performance system
        cluster_info = client.scheduler_info()
        n_workers = len(cluster_info['workers'])
        batch_size = calculate_optimal_batch_size(len(delayed_tasks), n_workers)
        logger.info(f"Using batch size: {batch_size} (optimized for {n_workers} workers)")
        logger.info(f"Expected total batches: {(len(delayed_tasks)-1)//batch_size + 1}")
        
        # For high-performance systems, we can process larger batches in parallel
        # This reduces scheduling overhead and improves throughput
        
        # Execute tasks in batches for better monitoring
        results = {}
        failed_tasks = []
        
        for i in range(0, len(delayed_tasks), batch_size):
            batch = delayed_tasks[i:i+batch_size]
            batch_num = i//batch_size + 1
            total_batches = (len(delayed_tasks)-1)//batch_size + 1
            
            logger.info(f"⚡ Processing batch {batch_num}/{total_batches} (tasks {i}-{min(i+batch_size-1, len(delayed_tasks)-1)})")
            logger.info("📈 Watch the Tasks tab in the dashboard to see progress!")
            
            # Compute batch with retry logic
            try:
                futures = client.compute(batch)
                batch_results = client.gather(futures)
                
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
                
                logger.info(f"✅ Batch {batch_num} completed - {successful}/{len(batch_results)} successful")
                
            except Exception as e:
                logger.error(f"Batch {batch_num} failed completely: {str(e)}")
                # Add all tasks in this batch to failed list
                failed_tasks.extend(range(i, min(i+batch_size, len(delayed_tasks))))
        
        # Report final results
        logger.info(f"🎉 Successfully processed {len(results)} time steps")
        if failed_tasks:
            logger.warning(f"⚠️  Failed to process {len(failed_tasks)} time steps: {failed_tasks[:10]}{'...' if len(failed_tasks) > 10 else ''}")
        
        # Clean up temporary file if created
        if climate_data_path == "temp_climate_data.nc" and os.path.exists(climate_data_path):
            os.remove(climate_data_path)
            logger.info("Cleaned up temporary climate data file")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in calculate_county_statistics_dask: {str(e)}")
        raise

def main():
    """Main execution function."""
    # Define paths using pathlib for better path handling
    climate_data_path = Path("output/climate_means/conus_historical_climate_means_2012-2014.nc")
    counties_path = Path("output/county_boundaries/contiguous_us_counties.parquet")
    output_dir = Path("output/county_climate_stats")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Setup Dask client optimized for high-performance system
    # Using 56 workers with 1 thread each for optimal I/O parallelism
    client = setup_dask_client(n_workers=56, threads_per_worker=1)
    
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
                
                results = calculate_county_statistics_dask(
                    climate_data,
                    counties,
                    var_name,
                    config['stat_type'],
                    client
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
        output_path = output_dir / 'county_climate_stats_dask.csv'
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
        client.close()
        
if __name__ == '__main__':
    main()