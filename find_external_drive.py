#!/usr/bin/env python3
"""
Helper script to identify external drive paths for climate data.
Run this script to see available drives and help configure the correct path.
"""

import os
import platform

def list_available_drives():
    """List available drives to help identify the correct external drive path."""
    system = platform.system()
    print(f"Detected system: {system}")
    print("=" * 50)
    
    if system == "Darwin":  # macOS
        volumes_path = "/Volumes"
        if os.path.exists(volumes_path):
            volumes = [d for d in os.listdir(volumes_path) if os.path.isdir(os.path.join(volumes_path, d))]
            print(f"Available volumes in {volumes_path}:")
            for vol in volumes:
                full_path = f"/Volumes/{vol}"
                print(f"  {full_path}")
                
                # Check if this might contain climate data
                potential_data_path = os.path.join(full_path, "climate_data")
                if os.path.exists(potential_data_path):
                    print(f"    ✓ Found climate_data directory!")
                
                # Look for common climate data patterns
                for var in ['tas', 'pr', 'tasmax', 'tasmin']:
                    var_path = os.path.join(full_path, var)
                    if os.path.exists(var_path):
                        print(f"    ✓ Found {var} directory")
    
    elif system == "Windows":
        import string
        available_drives = ['%s:' % d for d in string.ascii_uppercase if os.path.exists('%s:' % d)]
        print("Available drives:")
        for drive in available_drives:
            print(f"  {drive}")
            
            # Check if this might contain climate data
            potential_data_path = os.path.join(drive, "climate_data")
            if os.path.exists(potential_data_path):
                print(f"    ✓ Found climate_data directory!")
            
            # Look for common climate data patterns
            for var in ['tas', 'pr', 'tasmax', 'tasmin']:
                var_path = os.path.join(drive, var)
                if os.path.exists(var_path):
                    print(f"    ✓ Found {var} directory")
    
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
                                
                                # Check if this might contain climate data
                                potential_data_path = os.path.join(full_path, "climate_data")
                                if os.path.exists(potential_data_path):
                                    print(f"    ✓ Found climate_data directory!")
                                
                                # Look for common climate data patterns
                                for var in ['tas', 'pr', 'tasmax', 'tasmin']:
                                    var_path = os.path.join(full_path, var)
                                    if os.path.exists(var_path):
                                        print(f"    ✓ Found {var} directory")
                except PermissionError:
                    print(f"  Permission denied accessing {media_path}")

def search_for_climate_data(search_paths=None):
    """Search for climate data in common locations."""
    print("\n" + "=" * 50)
    print("SEARCHING FOR CLIMATE DATA FILES")
    print("=" * 50)
    
    if search_paths is None:
        system = platform.system()
        if system == "Darwin":
            search_paths = ["/Volumes"]
        elif system == "Windows":
            import string
            search_paths = ['%s:' % d for d in string.ascii_uppercase if os.path.exists('%s:' % d)]
        else:
            search_paths = ["/media", "/mnt", "/home"]
    
    climate_vars = ['tas', 'pr', 'tasmax', 'tasmin']
    scenarios = ['historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585']
    
    found_data = []
    
    for search_path in search_paths:
        if not os.path.exists(search_path):
            continue
            
        print(f"\nSearching in: {search_path}")
        
        try:
            for root, dirs, files in os.walk(search_path):
                # Look for NetCDF files with climate patterns
                nc_files = [f for f in files if f.endswith('.nc')]
                
                if nc_files:
                    # Check if this looks like climate data
                    climate_files = []
                    for f in nc_files:
                        for var in climate_vars:
                            if var in f and 'day' in f:
                                climate_files.append(f)
                                break
                    
                    if climate_files:
                        print(f"  Found climate data in: {root}")
                        print(f"    Sample files: {climate_files[:3]}")
                        if len(climate_files) > 3:
                            print(f"    ... and {len(climate_files)-3} more files")
                        found_data.append(root)
                        
                        # Check directory structure
                        parent_dir = os.path.dirname(root)
                        if any(scenario in root for scenario in scenarios):
                            print(f"    ✓ Scenario directory detected")
                        if any(var in root for var in climate_vars):
                            print(f"    ✓ Variable directory detected")
                
        except PermissionError:
            print(f"  Permission denied accessing {search_path}")
        except Exception as e:
            print(f"  Error searching {search_path}: {e}")
    
    if found_data:
        print(f"\n" + "=" * 50)
        print("SUMMARY - POTENTIAL CLIMATE DATA LOCATIONS:")
        print("=" * 50)
        for location in found_data:
            print(f"  {location}")
        
        print(f"\nTo configure climate_means.py:")
        print("1. Identify the base path that contains your climate data structure")
        print("2. Update EXTERNAL_DRIVE_PATH in climate_means.py")
        print("3. Ensure BASE_DATA_PATH points to the directory containing tas/, pr/, etc.")
    else:
        print("\nNo climate data files found in searched locations.")
        print("Please ensure your external drive is mounted and contains NetCDF files.")

if __name__ == "__main__":
    print("EXTERNAL DRIVE FINDER FOR CLIMATE DATA")
    print("=" * 50)
    
    list_available_drives()
    search_for_climate_data()
    
    print(f"\n" + "=" * 50)
    print("NEXT STEPS:")
    print("=" * 50)
    print("1. Identify your external drive path from the list above")
    print("2. Update EXTERNAL_DRIVE_PATH in climate_means.py")
    print("3. Run climate_means.py to process your data")
    print("\nExample configuration:")
    print("  EXTERNAL_DRIVE_PATH = '/Volumes/YourDriveName'")
    print("  BASE_DATA_PATH = f'{EXTERNAL_DRIVE_PATH}/climate_data'") 