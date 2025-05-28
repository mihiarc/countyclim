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
import time
import psutil
from tqdm import tqdm
import threading

warnings.filterwarnings('ignore', category=RuntimeWarning)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('county_stats.log')
    ]
)
logger = logging.getLogger(__name__)

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
        self.monitoring_active = False
        self.monitor_thread = None
        
    def start_monitoring(self):
        """Start performance monitoring."""
        self.start_time = time.time()
        self.monitoring_active = True
        
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
        
        logger.info("\nStage breakdown:")
        for stage, duration in summary['stage_times'].items():
            percentage = (duration / summary['total_time']) * 100 if summary['total_time'] > 0 else 0
            logger.info(f"  {stage}: {duration:.2f}s ({percentage:.1f}%)")
        
        logger.info("="*60)

# Global performance monitor
perf_monitor = PerformanceMonitor()

def load_climate_data(climate_data_path):
    """Load climate data from NetCDF file."""
    perf_monitor.start_stage("load_climate_data")
    
    logger.info(f"Loading climate data from {climate_data_path}")
    ds = xr.open_dataset(climate_data_path)
    
    logger.info(f"Climate variables: {list(ds.data_vars)}")
    logger.info(f"Dimensions: {ds.dims}")
    logger.info(f"Time range: {ds.time.values[0]} to {ds.time.values[-1]}")
    
    # Calculate data size
    total_size_gb = sum(var.nbytes for var in ds.data_vars.values()) / (1024**3)
    logger.info(f"Total data size: {total_size_gb:.2f} GB")
    
    perf_monitor.end_stage("load_climate_data")
    return ds

def load_county_boundaries(boundaries_path):
    """Load county boundaries from parquet file."""
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

def process_day(args):
    """Process a single day of climate data."""
    day_idx, data_array, counties_geom, transform_params, stat_type = args
    
    try:
        # Calculate zonal statistics for this day
        stats = zonal_stats(
            counties_geom,
            data_array[day_idx],
            affine=transform_params,
            stats=[stat_type],
            nodata=-9999
        )
        
        # Extract results
        results = [s[stat_type] if s[stat_type] is not None else 0.0 for s in stats]
        return day_idx, results
        
    except Exception as e:
        logger.error(f"Error processing day {day_idx}: {str(e)}")
        return day_idx, None

def calculate_county_statistics(climate_data, counties, variable_name, stat_type):
    """Calculate county statistics using multiprocessing."""
    perf_monitor.start_stage(f"calculate_stats_{variable_name}")
    logger.info(f"Calculating {stat_type} statistics for {variable_name}")
    
    try:
        # Get transform parameters
        transform_params = rasterio.transform.from_origin(
            climate_data.lon.min().values,
            climate_data.lat.max().values,
            np.abs(climate_data.lon[1].values - climate_data.lon[0].values),
            np.abs(climate_data.lat[1].values - climate_data.lat[0].values)
        )
        
        # Load all data into memory
        logger.info("Loading climate data into memory...")
        data_array = climate_data[variable_name].values
        logger.info(f"Data array shape: {data_array.shape}")
        
        # Extract county geometries
        counties_geom = counties.geometry.values
        
        # Prepare arguments for multiprocessing
        n_days = data_array.shape[0]
        n_workers = min(mp.cpu_count() - 1, n_days)
        logger.info(f"Using {n_workers} workers to process {n_days} days")
        
        args_list = [
            (day_idx, data_array, counties_geom, transform_params, stat_type)
            for day_idx in range(n_days)
        ]
        
        # Process using multiprocessing
        results = {}
        with mp.Pool(processes=n_workers) as pool:
            # Use tqdm to show progress
            with tqdm(total=n_days, desc=f"Processing {variable_name}") as pbar:
                for day_idx, day_results in pool.imap(process_day, args_list):
                    if day_results is not None:
                        # Convert day index to date
                        date = pd.Timestamp('2014-01-01') + pd.Timedelta(days=day_idx)
                        results[date] = day_results
                    pbar.update(1)
        
        perf_monitor.end_stage(f"calculate_stats_{variable_name}")
        return results
        
    except Exception as e:
        logger.error(f"Error in calculate_county_statistics: {str(e)}")
        raise

def main():
    """Main execution function."""
    # Define paths
    climate_data_path = "output/climate_means/conus_historical_climate_means_2012-2014.nc"
    counties_path = "output/county_boundaries/contiguous_us_counties.parquet"
    output_dir = "output/county_climate_stats"
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize performance monitoring
    perf_monitor.start_monitoring()
    
    try:
        # Load data
        logger.info("Loading input data...")
        climate_data = load_climate_data(climate_data_path)
        if climate_data is None:
            raise Exception("Failed to load climate data")
        
        counties = load_county_boundaries(counties_path)
        if counties is None or len(counties) == 0:
            raise Exception("Failed to load county boundaries")
            
        logger.info(f"Successfully loaded {len(counties)} counties")
        
        # Process precipitation data
        variables_to_process = {
            'pr_2014': {
                'stat_type': 'sum',
                'description': 'Total precipitation'
            }
        }
        
        # Process each variable
        all_results = {}
        for var_name, config in variables_to_process.items():
            logger.info(f"Processing {var_name}")
            results = calculate_county_statistics(
                climate_data,
                counties,
                var_name,
                config['stat_type']
            )
            all_results[var_name] = results
        
        # Convert results to DataFrame
        logger.info("Converting results to DataFrame...")
        
        # Create a DataFrame with proper structure
        dates = sorted(all_results['pr_2014'].keys())
        n_counties = len(counties)
        
        # Initialize DataFrame
        df_data = {}
        for var_name in all_results:
            var_data = []
            for date in dates:
                var_data.extend(all_results[var_name][date])
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
        
        # Save results
        output_path = os.path.join(output_dir, 'county_climate_stats.csv')
        output_df.to_csv(output_path, index=False)
        
        # Verify the file was created
        if os.path.exists(output_path):
            logger.info(f"Results successfully saved to {output_path}")
            logger.info(f"Output DataFrame shape: {output_df.shape}")
        else:
            raise Exception(f"Failed to save results to {output_path}")
        
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}", exc_info=True)
        raise
    finally:
        perf_monitor.stop_monitoring()
        perf_monitor.print_summary()
        
if __name__ == '__main__':
    main() 