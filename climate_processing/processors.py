"""
Core climate data processing logic.

This module contains the main ClimateProcessor class that orchestrates
the climate data processing workflow.
"""

import os
import glob
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import xarray as xr
import numpy as np
from dask.distributed import Client, LocalCluster
import psutil
import dask

from .config import ClimateConfig
from .regions import RegionManager, Region
from .periods import PeriodGenerator, ClimatePeriod
from .utils import (
    get_year_from_filename,
    format_output_filename,
    log_memory_usage,
    validate_external_drive
)


class ClimateProcessor:
    """Main processor class for climate data analysis."""
    
    def __init__(self, config: ClimateConfig):
        """
        Initialize the climate processor.
        
        Args:
            config: ClimateConfig instance with processing parameters
        """
        self.config = config
        self.region_manager = RegionManager()
        self.period_generator = PeriodGenerator(config.data_availability)
        self.client = None
        self.cluster = None
    
    def setup_dask(self) -> None:
        """Initialize Dask distributed computing cluster with optimized settings."""
        print(f"Initializing Dask cluster with {self.config.max_processes} workers")
        
        # Calculate optimal memory per worker
        total_memory_gb = psutil.virtual_memory().total / (1024**3)
        memory_per_worker = min(
            float(self.config.memory_limit.replace('GB', '')),
            (total_memory_gb * 0.8) / self.config.max_processes  # Use 80% of total memory
        )
        
        # Get Dask configuration from config
        dask_cfg = self.config.dask_config
        
        # Configure Dask settings for climate data processing
        dask.config.set({
            'array.slicing.split_large_chunks': True,
            'array.chunk-size': '128MB',
            'distributed.worker.memory.target': dask_cfg['memory_target'],
            'distributed.worker.memory.spill': dask_cfg['memory_spill'],
            'distributed.worker.memory.pause': dask_cfg['memory_pause'],
            'distributed.worker.memory.terminate': dask_cfg['memory_terminate'],
            'distributed.comm.timeouts.connect': dask_cfg['connect_timeout'],
            'distributed.comm.timeouts.tcp': dask_cfg['tcp_timeout'],
            'distributed.scheduler.allowed-failures': dask_cfg['allowed_failures'],
            'distributed.scheduler.work-stealing': dask_cfg['work_stealing'],
        })
        
        self.cluster = LocalCluster(
            n_workers=self.config.max_processes,
            threads_per_worker=dask_cfg['threads_per_worker'],
            memory_limit=f'{memory_per_worker:.1f}GB',
            processes=dask_cfg['processes'],
            dashboard_address=f":{dask_cfg['dashboard_port']}",
            silence_logs=dask_cfg['silence_logs'],
            local_directory='./dask-worker-space',  # Local scratch space
        )
        self.client = Client(self.cluster)
        
        print(f"Dask cluster initialized:")
        print(f"  Workers: {self.config.max_processes}")
        print(f"  Threads per worker: {dask_cfg['threads_per_worker']}")
        print(f"  Memory per worker: {memory_per_worker:.1f}GB")
        print(f"  Total cluster memory: {memory_per_worker * self.config.max_processes:.1f}GB")
        print(f"  Dashboard: {self.client.dashboard_link}")
        print(f"  Memory management: target={dask_cfg['memory_target']}, spill={dask_cfg['memory_spill']}")
        
        # Warm up the cluster
        self.client.run(lambda: None)
    
    def cleanup_dask(self) -> None:
        """Clean up Dask cluster and client."""
        if self.client:
            self.client.close()
        if self.cluster:
            self.cluster.close()
    
    def process(self) -> None:
        """Execute the main climate data processing workflow."""
        start_time = datetime.now()
        print(f"Starting climate data processing at {start_time}")
        print(f"Processing scenario: {self.config.active_scenario}")
        print(f"Processing variables: {', '.join(self.config.active_variables)}")
        print(f"Processing regions: {', '.join(self.config.active_regions)}")
        
        try:
            # Validate paths
            if not self._validate_setup():
                return
            
            # Setup Dask if parallel processing is enabled
            if self.config.parallel_processing:
                self.setup_dask()
            
            # Generate climate periods
            periods = self.period_generator.generate_periods(self.config.active_scenario)
            if not periods:
                print("No climate periods to process")
                return
            
            # Get file patterns for active variables only
            scenario_patterns = self.config.get_active_scenario_patterns()
            
            if not scenario_patterns:
                print("No variables to process based on current configuration")
                return
            
            # Process only active regions
            for region_key in self.config.active_regions:
                if region_key not in self.region_manager.list_regions():
                    print(f"Warning: Region '{region_key}' not found in region manager")
                    continue
                    
                region = self.region_manager.get_region(region_key)
                print(f"\nProcessing region: {region.name}")
                
                # Process region data
                region_dataset = self._process_region(
                    region, periods, scenario_patterns
                )
                
                if region_dataset:
                    # Save the processed data
                    self._save_region_data(region, region_dataset)
                else:
                    print(f"  No data processed for region {region.name}")
            
            end_time = datetime.now()
            runtime = end_time - start_time
            print(f"\nProcessing complete!")
            print(f"Total runtime: {runtime}")
            print(f"Processed variables: {', '.join(self.config.active_variables)}")
            print(f"Processed regions: {', '.join(self.config.active_regions)}")
            
        finally:
            # Always cleanup Dask resources
            if self.config.parallel_processing:
                self.cleanup_dask()
    
    def _validate_setup(self) -> bool:
        """Validate configuration and external paths."""
        try:
            # Validate configuration
            self.config.validate()
            
            # Validate external drive
            return validate_external_drive(
                self.config.external_drive_path,
                self.config.base_data_path
            )
        except Exception as e:
            print(f"Validation error: {e}")
            return False
    
    def _process_region(self, region: Region, periods: List[ClimatePeriod],
                       scenario_patterns: Dict[str, str]) -> Optional[xr.Dataset]:
        """
        Process all active variables and periods for a specific region.
        
        Args:
            region: Region to process
            periods: List of climate periods
            scenario_patterns: File patterns for each active variable
            
        Returns:
            xarray Dataset with processed climate data, or None if processing failed
        """
        region_dataset = xr.Dataset()
        
        # Process each active variable
        for var_name, file_pattern in scenario_patterns.items():
            print(f"\n  Processing {var_name} for {region.name}...")
            
            # Process each climate period
            for period in periods:
                climate_avg = self._process_period_region(
                    period, var_name, file_pattern, region
                )
                
                if climate_avg is not None:
                    # Apply unit conversions
                    climate_avg = self._apply_unit_conversions(climate_avg, var_name)
                    
                    # Add to dataset
                    var_id = f"{var_name}_{period.target_year}"
                    region_dataset[var_id] = climate_avg
                    print(f"    Added {var_name} for target year {period.target_year}")
        
        return region_dataset if len(region_dataset.data_vars) > 0 else None
    
    def _process_period_region(self, period: ClimatePeriod, variable_name: str,
                             file_pattern: str, region: Region) -> Optional[xr.DataArray]:
        """
        Process a climate period for a specific variable and region.
        
        Args:
            period: Climate period to process
            variable_name: Name of the climate variable
            file_pattern: File pattern for the variable
            region: Region to extract
            
        Returns:
            xarray DataArray with climate average, or None if processing failed
        """
        print(f"    Processing period {period} for {variable_name}")
        
        # Get all files for this period
        files = self._get_files_for_period(file_pattern, period.start_year, period.end_year)
        
        if not files:
            print(f"    No files found for {variable_name} during {period.period_name}")
            return None
        
        # Process files
        results = []
        for f in files:
            year, data = self._process_file(f, variable_name, region)
            if year is not None:
                results.append((year, data))
        
        if not results:
            print(f"    No valid data for {variable_name} during {period.period_name}")
            return None
        
        # Calculate climate normal
        climate_avg = self._calculate_climate_normal(results, period)
        
        return climate_avg
    
    def _get_files_for_period(self, file_pattern: str, start_year: int, end_year: int) -> List[str]:
        """Get all files for a given climate period."""
        all_files = glob.glob(file_pattern)
        period_files = []
        
        for f in all_files:
            year = get_year_from_filename(f)
            if year and start_year <= year <= end_year:
                period_files.append(f)
        
        if not period_files:
            print(f"    Warning: No files found matching pattern {file_pattern} "
                  f"for years {start_year}-{end_year}")
        else:
            print(f"    Found {len(period_files)} files for period {start_year}-{end_year}")
            
        return sorted(period_files)
    
    def _process_file(self, file_path: str, variable_name: str,
                     region: Region) -> Tuple[Optional[int], Optional[xr.DataArray]]:
        """Process a single NetCDF file to get daily values for a specific region."""
        try:
            # Open the file with dask chunking
            ds = xr.open_dataset(file_path, chunks=self.config.chunk_size)
            
            # Extract region
            region_ds = region.extract_from_dataset(ds)
            
            # Get the daily values
            daily_data = region_ds[variable_name]
            
            # Get year from filename
            year = get_year_from_filename(file_path)
            
            return year, daily_data
            
        except Exception as e:
            print(f"    Error processing {file_path}: {e}")
            return None, None
    
    def _calculate_climate_normal(self, results: List[Tuple[int, xr.DataArray]],
                                period: ClimatePeriod) -> xr.DataArray:
        """Calculate 30-year climate normal from daily data."""
        # Extract the data arrays and align them by day of year
        years = [year for year, _ in results]
        print(f"    Processing years: {sorted(years)} (total: {len(years)} years)")
        
        # First, ensure all arrays have dayofyear as a coordinate
        data_arrays = []
        for _, data in results:
            # Calculate dayofyear while preserving original structure
            dayofyear = data.time.dt.dayofyear.data
            # Add dayofyear as a coordinate without changing dimensions
            with_doy = data.assign_coords(dayofyear=('time', dayofyear))
            data_arrays.append(with_doy)
        
        # Stack the arrays along a new 'year' dimension
        print("    Concatenating data arrays...")
        stacked_data = xr.concat(data_arrays, dim=xr.DataArray(years, dims='year', name='year'))
        
        # Compute 30-year climate normal (mean over years for each unique dayofyear)
        print(f"    Computing 30-year climate normal for target year {period.target_year}...")
        climate_avg = stacked_data.groupby('dayofyear').mean(dim=['year', 'time'])
        
        # Create new time coordinates using the target year as reference
        days = climate_avg.dayofyear.values
        dates = [np.datetime64(f'{period.target_year}-01-01') + 
                np.timedelta64(int(d-1), 'D') for d in days]
        
        # Assign new time coordinate
        climate_avg = climate_avg.assign_coords(time=('dayofyear', dates))
        climate_avg = climate_avg.swap_dims({'dayofyear': 'time'})
        
        # Add metadata
        climate_avg.attrs.update({
            'climate_period_start': period.start_year,
            'climate_period_end': period.end_year,
            'climate_target_year': period.target_year,
            'climate_period_length': period.length,
            'description': f'30-year climate normal based on {period.start_year}-{period.end_year}'
        })
        
        return climate_avg
    
    def _apply_unit_conversions(self, data: xr.DataArray, variable_name: str) -> xr.DataArray:
        """Apply unit conversions to climate data."""
        if variable_name in ['tas', 'tasmax', 'tasmin']:
            # Convert from Kelvin to Celsius
            data = data - 273.15
            data.attrs['units'] = 'degC'
        elif variable_name == 'pr':
            # Convert from kg m^-2 s^-1 to mm/day
            data = data * 86400
            data.attrs['units'] = 'mm/day'
        
        return data
    
    def _save_region_data(self, region: Region, dataset: xr.Dataset) -> None:
        """Save processed region data to NetCDF file."""
        # Prepare output filename
        is_historical = self.config.active_scenario == 'historical'
        
        # Include variable information in filename if not processing all variables
        if len(self.config.active_variables) < 4:  # Less than all 4 variables
            var_suffix = "_" + "_".join(sorted(self.config.active_variables))
        else:
            var_suffix = ""
        
        output_filename = format_output_filename(
            region.name, self.config.active_scenario, is_historical, var_suffix
        )
        output_path = os.path.join(self.config.output_dir, output_filename)
        
        # Convert longitudes if specified
        if region.convert_longitudes:
            print(f"  Converting longitudes for {region.name} from 0-360 to -180 to 180 format...")
            dataset = RegionManager.convert_longitudes_to_standard(dataset)
        
        # Add global attributes
        dataset.attrs.update({
            'title': f'30-year Climate Normals for {region.name}',
            'scenario': self.config.active_scenario,
            'variables_processed': ', '.join(self.config.active_variables),
            'region_processed': region.name,
            'methodology': '30-year moving window climate normals',
            'temporal_coverage': '1980-2014' if is_historical else '2015-2100',
            'spatial_coverage': region.name,
            'created': datetime.now().isoformat(),
            'description': 'Annual climate measures based on 30-year moving windows. '
                         'Each year represents the climate normal based on the preceding 30 years.',
            'variables': 'tas (mean temperature), tasmax (maximum temperature), '
                       'tasmin (minimum temperature), pr (precipitation)',
            'units_temperature': 'degrees Celsius',
            'units_precipitation': 'mm/day'
        })
        
        # Save the dataset
        print(f"  Computing and saving {region.name} data...")
        dataset = dataset.compute()  # Explicitly compute before saving
        dataset.to_netcdf(output_path)
        print(f"  Saved {region.name} data to {output_path}")
        print(f"    Variables: {list(dataset.data_vars.keys())}")
        print(f"    Time range: {len([v for v in dataset.data_vars.keys() if any(v.startswith(var + '_') for var in self.config.active_variables)])} "
              f"annual climate measures")
        
        # Log memory usage after saving
        log_memory_usage() 