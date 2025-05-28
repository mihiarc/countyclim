"""
Climate Calculations Module

Handles the core climate data calculations including 30-year climate normals.
"""

import logging
import numpy as np
import xarray as xr
from typing import List, Optional, Tuple
from pathlib import Path

from .regions import REGION_BOUNDS, extract_region
from .dask_utils import get_optimal_chunks, open_dataset_optimized
from .coordinates import convert_longitudes_to_standard

logger = logging.getLogger(__name__)


def process_file_lazy_wrapper(file_path: str, variable_name: str, region_key: str, 
                             file_handler) -> Tuple[Optional[int], Optional[xr.DataArray]]:
    """
    Wrapper for processing individual files with error handling.
    
    Args:
        file_path: Path to NetCDF file
        variable_name: Climate variable name
        region_key: Region identifier  
        file_handler: File handler instance
        
    Returns:
        Tuple of (year, processed_data_array) or (None, None) if failed
    """
    try:
        # Extract year from filename
        year = file_handler.extract_year_from_filename(file_path)
        if not year:
            logger.warning(f"Could not extract year from {Path(file_path).name}")
            return None, None
        
        # Determine optimal chunks
        chunks = get_optimal_chunks(file_path, {'target_chunk_size': 75})
        
        # Open dataset with optimization
        ds = open_dataset_optimized(file_path, chunks)
        
        if variable_name not in ds.data_vars:
            logger.warning(f"{variable_name} not found in {Path(file_path).name}")
            ds.close()
            return None, None
        
        # Extract the variable
        data_var = ds[variable_name]
        
        # Convert coordinates if needed
        if REGION_BOUNDS[region_key].get('convert_longitudes', False):
            ds_converted = convert_longitudes_to_standard(ds)
            data_var = ds_converted[variable_name]
        
        # Extract region
        region_data = extract_region(ds if not REGION_BOUNDS[region_key].get('convert_longitudes', False) else ds_converted, 
                                   REGION_BOUNDS[region_key])
        
        if variable_name not in region_data.data_vars:
            logger.warning(f"No data after regional extraction for {variable_name} in {region_key}")
            ds.close()
            return None, None
        
        region_var = region_data[variable_name]
        
        # Check if we have valid data
        if region_var.isnull().all():
            logger.warning(f"All data is null for {variable_name} {year} in {region_key}")
            ds.close()
            return None, None
        
        # Calculate daily climatology (mean over time dimension)
        try:
            # Group by day of year and calculate mean
            if 'time' in region_var.dims:
                daily_clim = region_var.groupby('time.dayofyear').mean('time')
            else:
                logger.warning(f"No time dimension found in {variable_name} for {year}")
                ds.close()
                return None, None
            
            # Add year coordinate for tracking
            daily_clim = daily_clim.assign_coords(year=year)
            
            # Close the dataset to free memory
            ds.close()
            
            logger.debug(f"Processed {variable_name} for {year} in {REGION_BOUNDS[region_key]['name']}")
            return year, daily_clim
            
        except Exception as e:
            logger.error(f"Error processing climatology for {variable_name} {year}: {e}")
            ds.close()
            return None, None
            
    except Exception as e:
        logger.error(f"Error processing file {Path(file_path).name}: {e}")
        return None, None


def compute_climate_normal_efficiently(data_arrays: List[xr.DataArray], years: List[int], 
                                     target_year: int) -> xr.DataArray:
    """
    Compute 30-year climate normal for a target year using efficient xarray operations.
    
    Args:
        data_arrays: List of daily climatology data arrays
        years: List of corresponding years
        target_year: Target year for the climate normal
        
    Returns:
        Climate normal data array
    """
    logger.debug(f"Computing 30-year normal for target year {target_year}")
    logger.debug(f"Using {len(data_arrays)} years of data")
    
    if not data_arrays:
        logger.warning(f"No data arrays provided for target year {target_year}")
        return None
    
    try:
        # Stack all years along a new 'year' dimension
        # First, ensure all arrays have the same structure
        aligned_arrays = []
        for i, da in enumerate(data_arrays):
            if 'year' not in da.coords:
                da = da.assign_coords(year=years[i])
            aligned_arrays.append(da)
        
        # Concatenate along year dimension
        stacked = xr.concat(aligned_arrays, dim='year')
        
        # Calculate mean across years (30-year average)
        climate_normal = stacked.mean(dim='year')
        
        # Add target year as coordinate
        climate_normal = climate_normal.assign_coords(target_year=target_year)
        
        # Add metadata
        climate_normal.attrs.update({
            'long_name': f'30-year climate normal for {target_year}',
            'description': f'Average of daily climatologies from {min(years)} to {max(years)}',
            'target_year': target_year,
            'source_years': f"{min(years)}-{max(years)}",
            'number_of_years': len(years)
        })
        
        logger.debug(f"Successfully computed climate normal for {target_year}")
        return climate_normal
        
    except Exception as e:
        logger.error(f"Error computing climate normal for {target_year}: {e}")
        return None


def process_period_region_optimized(period_info: Tuple[int, int, int, str], variable_name: str, 
                                  file_handler, region_key: str) -> Optional[xr.DataArray]:
    """
    Process a climate period for a specific region and variable with optimization.
    
    Args:
        period_info: Tuple of (start_year, end_year, target_year, period_name)
        variable_name: Climate variable name
        file_handler: File handler instance
        region_key: Region identifier
        
    Returns:
        Processed climate normal data array or None if failed
    """
    start_year, end_year, target_year, period_name = period_info
    
    logger.debug(f"Processing {variable_name} for {period_name} in {REGION_BOUNDS[region_key]['name']}")
    logger.debug(f"Years: {start_year}-{end_year}, target: {target_year}")
    
    try:
        # Get files for this period
        scenario = period_name.split('_')[0]  # Extract scenario from period name
        files = file_handler.get_files_for_period(variable_name, scenario, start_year, end_year)
        
        if not files:
            logger.warning(f"No files found for {variable_name} {scenario} {start_year}-{end_year}")
            return None
        
        if len(files) < 20:  # Require at least 20 years for a meaningful climate normal
            logger.warning(f"Insufficient data for {variable_name} {scenario} {start_year}-{end_year}: "
                         f"only {len(files)} files found")
            return None
        
        # Process files to get daily climatologies
        data_arrays = []
        years = []
        
        for file_path in files:
            year, daily_clim = process_file_lazy_wrapper(file_path, variable_name, region_key, file_handler)
            if year is not None and daily_clim is not None:
                data_arrays.append(daily_clim)
                years.append(year)
        
        if not data_arrays:
            logger.warning(f"No valid data processed for {variable_name} {period_name} in {region_key}")
            return None
        
        logger.info(f"Processing {len(data_arrays)} years of data for {variable_name} {period_name} in {region_key}")
        
        # Compute 30-year climate normal
        climate_normal = compute_climate_normal_efficiently(data_arrays, years, target_year)
        
        if climate_normal is not None:
            # Add additional metadata
            climate_normal.attrs.update({
                'variable': variable_name,
                'region': REGION_BOUNDS[region_key]['name'],
                'region_key': region_key,
                'period': period_name,
                'processing_method': 'daily_climatology_30year_mean'
            })
            
            logger.debug(f"Successfully processed {variable_name} for {period_name} in {region_key}")
            return climate_normal
        else:
            logger.warning(f"Failed to compute climate normal for {variable_name} {period_name} in {region_key}")
            return None
            
    except Exception as e:
        logger.error(f"Error processing {variable_name} {period_name} in {region_key}: {e}")
        return None


def validate_climate_data(data_array: xr.DataArray, variable_name: str) -> bool:
    """
    Validate climate data for reasonable values and completeness.
    
    Args:
        data_array: Climate data array to validate
        variable_name: Name of the climate variable
        
    Returns:
        True if data is valid, False otherwise
    """
    try:
        # Check for completely missing data
        if data_array.isnull().all():
            logger.warning(f"All data is missing for {variable_name}")
            return False
        
        # Check missing data percentage
        missing_percent = float(data_array.isnull().mean() * 100)
        if missing_percent > 50:
            logger.warning(f"Too much missing data for {variable_name}: {missing_percent:.1f}%")
            return False
        elif missing_percent > 10:
            logger.info(f"Some missing data for {variable_name}: {missing_percent:.1f}%")
        
        # Variable-specific validation
        min_val = float(data_array.min())
        max_val = float(data_array.max())
        
        if variable_name in ['tas', 'tasmax', 'tasmin']:
            # Temperature validation (assuming Kelvin input)
            if min_val < 150 or max_val > 350:
                logger.warning(f"Temperature values outside typical range for {variable_name}: "
                             f"{min_val:.1f} to {max_val:.1f} K")
            if min_val < 0:
                logger.error(f"Negative temperature values found for {variable_name}: {min_val:.1f}")
                return False
                
        elif variable_name == 'pr':
            # Precipitation validation
            if min_val < 0:
                logger.error(f"Negative precipitation values found: {min_val:.6f}")
                return False
            if max_val > 1:  # kg m^-2 s^-1 (very high but possible)
                logger.info(f"Very high precipitation values: {max_val:.6f} kg m^-2 s^-1")
        
        # Check for infinite values
        if not np.isfinite(data_array.values).all():
            logger.error(f"Data contains infinite values for {variable_name}")
            return False
        
        logger.debug(f"Data validation passed for {variable_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error during data validation for {variable_name}: {e}")
        return False 