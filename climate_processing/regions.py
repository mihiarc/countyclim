"""
Regional definitions and coordinate reference system management.

This module defines the geographical regions for climate data processing
and their associated coordinate reference systems.
"""

from typing import Dict, Any, Optional
import numpy as np
import xarray as xr


class Region:
    """Class representing a geographical region with bounds and CRS information."""
    
    def __init__(self, 
                 name: str,
                 lon_min: float,
                 lon_max: float,
                 lat_min: float,
                 lat_max: float,
                 convert_longitudes: bool = True,
                 crs_info: Optional[Dict[str, Any]] = None):
        """
        Initialize a region.
        
        Args:
            name: Human-readable name of the region
            lon_min: Minimum longitude (in 0-360 system)
            lon_max: Maximum longitude (in 0-360 system)
            lat_min: Minimum latitude
            lat_max: Maximum latitude
            convert_longitudes: Whether to convert longitudes to -180/180 system
            crs_info: Coordinate reference system information
        """
        self.name = name
        self.lon_min = lon_min
        self.lon_max = lon_max
        self.lat_min = lat_min
        self.lat_max = lat_max
        self.convert_longitudes = convert_longitudes
        self.crs_info = crs_info or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert region to dictionary."""
        return {
            'name': self.name,
            'lon_min': self.lon_min,
            'lon_max': self.lon_max,
            'lat_min': self.lat_min,
            'lat_max': self.lat_max,
            'convert_longitudes': self.convert_longitudes,
            **self.crs_info
        }
    
    def extract_from_dataset(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Extract this region from a dataset.
        
        Args:
            ds: xarray Dataset to extract from
            
        Returns:
            xarray Dataset containing only data within region bounds
        """
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
                'lon_min': self.lon_min,
                'lon_max': self.lon_max
            }
        else:
            # Convert from 0-360 to -180-180 system
            lon_bounds = {
                'lon_min': self.lon_min - 360 if self.lon_min > 180 else self.lon_min,
                'lon_max': self.lon_max - 360 if self.lon_max > 180 else self.lon_max
            }
        
        # Handle the case where we cross the 0/360 or -180/180 boundary
        if lon_bounds['lon_min'] > lon_bounds['lon_max']:
            region_ds = ds.where(
                ((ds[lon_name] >= lon_bounds['lon_min']) | 
                 (ds[lon_name] <= lon_bounds['lon_max'])) & 
                (ds[lat_name] >= self.lat_min) & 
                (ds[lat_name] <= self.lat_max), 
                drop=True
            )
        else:
            region_ds = ds.where(
                (ds[lon_name] >= lon_bounds['lon_min']) & 
                (ds[lon_name] <= lon_bounds['lon_max']) & 
                (ds[lat_name] >= self.lat_min) & 
                (ds[lat_name] <= self.lat_max), 
                drop=True
            )
        
        # Check if we have data
        if region_ds[lon_name].size == 0 or region_ds[lat_name].size == 0:
            print(f"Warning: No data found within region bounds for {self.name}")
            print(f"Dataset longitude range: {lon_min} to {lon_max}")
            print(f"Region bounds: {self.lon_min} to {self.lon_max} (original)")
        
        return region_ds
    
    def __repr__(self) -> str:
        """String representation of region."""
        return f"Region('{self.name}', lon=[{self.lon_min}, {self.lon_max}], lat=[{self.lat_min}, {self.lat_max}])"


class RegionManager:
    """Manager class for handling multiple regions."""
    
    def __init__(self):
        """Initialize region manager with predefined regions."""
        self.regions = self._create_regions()
    
    def _create_regions(self) -> Dict[str, Region]:
        """Create the predefined regions."""
        return {
            'CONUS': Region(
                name='CONUS',
                lon_min=234,   # 234°E in 0-360 system (-126°E in -180/180)
                lon_max=294,   # 294°E in 0-360 system (-66°E in -180/180)
                lat_min=24.0,  # Extended south to fully cover Florida
                lat_max=50.0,  # Extended north to ensure coverage
                convert_longitudes=True,
                crs_info={
                    'crs_type': 'epsg',
                    'crs_value': 5070,  # NAD83 / Conus Albers
                    'central_longitude': -96,
                    'central_latitude': 37.5,
                    'extent': [-125, -65, 25, 50]  # West, East, South, North
                }
            ),
            'AK': Region(
                name='Alaska',
                lon_min=170,   # 170°E in 0-360 system
                lon_max=235,   # 235°E in 0-360 system - extended to include SE Alaska
                lat_min=50.0,
                lat_max=72.0,
                convert_longitudes=True,
                crs_info={
                    'crs_type': 'epsg',
                    'crs_value': 3338,  # NAD83 / Alaska Albers
                    'central_longitude': -154,
                    'central_latitude': 50,
                    'extent': [-170, -130, 50, 72]
                }
            ),
            'HI': Region(
                name='Hawaii and Islands',
                lon_min=181.63,   # 181.63 in 0-360 system
                lon_max=205.20,   # 205.20 in 0-360 system
                lat_min=18.92,
                lat_max=28.45,
                convert_longitudes=True,
                crs_info={
                    'crs_type': 'proj4',
                    'crs_value': "+proj=aea +lat_1=8 +lat_2=18 +lat_0=13 +lon_0=157 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs",
                    'central_longitude': -157,
                    'central_latitude': 20,
                    'extent': [-178, -155, 18, 29]
                }
            ),
            'PRVI': Region(
                name='Puerto Rico and U.S. Virgin Islands',
                lon_min=292.03,   # -67.97 in 0-360 system
                lon_max=295.49,   # -64.51 in 0-360 system
                lat_min=17.62,
                lat_max=18.57,
                convert_longitudes=True,
                crs_info={
                    'crs_type': 'epsg',
                    'crs_value': 6566,  # NAD83(2011) / Puerto Rico and Virgin Islands
                    'central_longitude': -66,
                    'central_latitude': 18,
                    'extent': [-68, -64, 17, 19]
                }
            ),
            'GU': Region(
                name='Guam and Northern Mariana Islands',
                lon_min=144.58,   # 144.58°E in 0-360 system
                lon_max=146.12,   # 146.12°E in 0-360 system
                lat_min=13.18,
                lat_max=20.61,
                convert_longitudes=True,
                crs_info={
                    'crs_type': 'epsg',
                    'crs_value': 32655,  # WGS 84 / UTM zone 55N
                    'central_longitude': 147,
                    'central_latitude': 13.5,
                    'extent': [144, 147, 13, 21]
                }
            )
        }
    
    def get_region(self, region_key: str) -> Region:
        """
        Get a region by its key.
        
        Args:
            region_key: Region identifier (CONUS, AK, HI, PRVI, GU)
            
        Returns:
            Region object
            
        Raises:
            KeyError: If region_key is not found
        """
        if region_key not in self.regions:
            raise KeyError(f"Region '{region_key}' not found. "
                         f"Available regions: {list(self.regions.keys())}")
        return self.regions[region_key]
    
    def list_regions(self) -> list:
        """Get list of available region keys."""
        return list(self.regions.keys())
    
    def get_all_regions(self) -> Dict[str, Region]:
        """Get all regions."""
        return self.regions
    
    @staticmethod
    def convert_longitudes_to_standard(ds: xr.Dataset) -> xr.Dataset:
        """
        Convert longitudes from 0-360 format to standard -180 to 180 format.
        
        Args:
            ds: Dataset with longitude coordinates in 0-360 format
            
        Returns:
            Dataset with longitude coordinates in -180 to 180 format
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