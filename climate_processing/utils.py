"""
Utility functions for climate data processing.

This module provides helper functions for path validation, system detection,
and other common utilities.
"""

import os
import platform
from pathlib import Path
from typing import Optional, List


def list_available_drives() -> None:
    """List available drives to help identify the correct external drive path."""
    system = platform.system()
    print(f"\nDetected system: {system}")
    
    if system == "Darwin":  # macOS
        volumes_path = "/Volumes"
        if os.path.exists(volumes_path):
            volumes = [d for d in os.listdir(volumes_path) if os.path.isdir(os.path.join(volumes_path, d))]
            print(f"Available volumes in {volumes_path}:")
            for vol in volumes:
                print(f"  /Volumes/{vol}")
    
    elif system == "Windows":
        import string
        available_drives = ['%s:' % d for d in string.ascii_uppercase if os.path.exists('%s:' % d)]
        print("Available drives:")
        for drive in available_drives:
            print(f"  {drive}")
    
    elif system == "Linux":
        media_paths = ["/media", "/mnt"]
        for media_path in media_paths:
            if os.path.exists(media_path):
                try:
                    mounts = os.listdir(media_path)
                    if mounts:
                        print(f"Available mounts in {media_path}:")
                        for mount in mounts:
                            full_path = os.path.join(media_path, mount)
                            if os.path.isdir(full_path):
                                print(f"  {full_path}")
                except PermissionError:
                    print(f"  Permission denied accessing {media_path}")


def validate_external_drive(external_drive_path: str, base_data_path: str) -> bool:
    """
    Validate that the external drive path exists and is accessible.
    
    Args:
        external_drive_path: Path to the external drive
        base_data_path: Path to the base data directory
        
    Returns:
        True if paths are valid, False otherwise
    """
    if not os.path.exists(external_drive_path):
        print(f"ERROR: External drive path does not exist: {external_drive_path}")
        print("Please update CLIMATE_EXTERNAL_DRIVE environment variable or config to point to your external drive.")
        print("Common paths:")
        print("  macOS: /Volumes/YourDriveName")
        print("  Windows: D: or E:")
        print("  Linux: /media/username/drivename or /mnt/external")
        
        # List available drives to help user
        list_available_drives()
        return False
    
    if not os.path.exists(base_data_path):
        print(f"WARNING: Base data path does not exist: {base_data_path}")
        print("Please ensure your climate data is organized in the expected directory structure.")
        print("Expected structure:")
        print(f"  {base_data_path}/")
        print("    ├── tas/")
        print("    │   ├── historical/")
        print("    │   ├── ssp126/")
        print("    │   ├── ssp245/")
        print("    │   ├── ssp370/")
        print("    │   └── ssp585/")
        print("    ├── tasmax/")
        print("    ├── tasmin/")
        print("    └── pr/")
        print("  OR if data is in a different structure, update CLIMATE_BASE_DATA_PATH accordingly.")
        return False
    
    print(f"✓ External drive path validated: {external_drive_path}")
    print(f"✓ Base data path found: {base_data_path}")
    return True


def get_year_from_filename(filename: str) -> Optional[int]:
    """
    Extract year from climate data filename.
    
    Args:
        filename: Path to the climate data file
        
    Returns:
        Year as integer, or None if not found
    """
    base = os.path.basename(filename)
    
    # For tas files: tas_day_NorESM2-LM_historical_r1i1p1f1_gn_2012.nc
    # For pr files: pr_day_NorESM2-LM_historical_r1i1p1f1_gn_2012_v1.1.nc
    parts = base.split('_')
    
    # Look for year in parts that could contain it
    for part in parts:
        # Check if this part starts with '19' or '20' (for years 1900-2099)
        if (part.startswith('19') or part.startswith('20')) and len(part) >= 4:
            # Extract the first 4 characters if the part has additional content
            year_str = part[:4]
            try:
                year = int(year_str)
                # Validate year is in reasonable range
                if 1900 <= year <= 2100:
                    return year
            except ValueError:
                continue
    
    print(f"Warning: Could not extract year from {filename}")
    return None


def format_output_filename(region_name: str, scenario: str, is_historical: bool = True) -> str:
    """
    Generate standardized output filename for climate data.
    
    Args:
        region_name: Name of the region
        scenario: Climate scenario name
        is_historical: Whether this is historical data
        
    Returns:
        Formatted filename
    """
    # Clean region name for filename
    clean_region = region_name.lower().replace(' ', '_').replace('.', '')
    
    if is_historical:
        return f'{clean_region}_{scenario}_30yr_climate_normals_1980-2014.nc'
    else:
        return f'{clean_region}_{scenario}_30yr_climate_normals_2015-2100.nc'


def log_memory_usage() -> None:
    """Log current memory usage information."""
    try:
        import psutil
        memory = psutil.virtual_memory()
        print(f"Memory usage: {memory.percent:.1f}% ({memory.used / (1024**3):.2f} GB used / "
              f"{memory.total / (1024**3):.2f} GB total)")
    except ImportError:
        print("psutil not available for memory monitoring")


def ensure_directory_exists(path: str) -> None:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path to ensure exists
    """
    Path(path).mkdir(parents=True, exist_ok=True)


def validate_netcdf_file(filepath: str) -> bool:
    """
    Validate that a NetCDF file exists and can be opened.
    
    Args:
        filepath: Path to the NetCDF file
        
    Returns:
        True if file is valid, False otherwise
    """
    if not os.path.exists(filepath):
        return False
    
    try:
        import xarray as xr
        with xr.open_dataset(filepath) as ds:
            # Basic validation - check if it has dimensions
            return len(ds.dims) > 0
    except Exception:
        return False 