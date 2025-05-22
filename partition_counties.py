#!/usr/bin/env python3
"""
County Partitioning Script

This script partitions the US county shapefile into 5 regions:
- Alaska
- Hawaii
- Guam and the Mariana Islands
- Puerto Rico and the US Virgin Islands
- Contiguous US (CONUS)

Each region is exported as a geoparquet file in its original coordinate system.
"""

import os
import geopandas as gpd
from shapely.geometry import box

# Input shapefile
INPUT_SHAPEFILE = 'data/tl_2024_us_county/tl_2024_us_county.shp'

# Output directory
OUTPUT_DIR = 'output/county_boundaries'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Region definitions with bounding boxes in lat/lon coordinates
# These use -180 to 180 longitude format
REGIONS = {
    'AK': {
        'name': 'Alaska',
        'lon_min': -190,  # -190°E in -180 to 180 system
        'lon_max': -125,  # -125°E in -180 to 180 system
        'lat_min': 50.0,
        'lat_max': 72.0,
        'state_fips': ['02']  # Alaska FIPS code
    },
    'HI': {
        'name': 'Hawaii',
        'lon_min': -178.37,  # -178.37°E in -180 to 180 system (181.63 - 360)
        'lon_max': -154.8,   # -154.8°E in -180 to 180 system (205.20 - 360)
        'lat_min': 18.92,
        'lat_max': 28.45,
        'state_fips': ['15']  # Hawaii FIPS code
    },
    'GU': {
        'name': 'Guam_and_Mariana_Islands',
        'lon_min': 144.58,  # 144.58°E in -180 to 180 system
        'lon_max': 146.12,  # 146.12°E in -180 to 180 system
        'lat_min': 13.18,
        'lat_max': 20.61,
        'state_fips': ['66', '69']  # Guam (66) and Northern Mariana Islands (69) FIPS codes
    },
    'PRVI': {
        'name': 'Puerto_Rico_and_Virgin_Islands',
        'lon_min': -67.97,  # -67.97°E in -180 to 180 system (292.03 - 360)
        'lon_max': -64.51,  # -64.51°E in -180 to 180 system (295.49 - 360)
        'lat_min': 17.62,
        'lat_max': 18.57,
        'state_fips': ['72', '78']  # Puerto Rico (72) and U.S. Virgin Islands (78) FIPS codes
    },
    'CONUS': {
        'name': 'Contiguous_US',
        'lon_min': -125,  # -125°E in -180 to 180 system
        'lon_max': -65,   # -65°E in -180 to 180 system
        'lat_min': 24.5,
        'lat_max': 49.4,
        # All state FIPS except those in other regions
        'is_default': True  # This will contain counties not in other regions
    }
}

def create_spatial_filter(region):
    """Create a spatial filter (bounding box) for the specified region."""
    return box(
        region['lon_min'], 
        region['lat_min'], 
        region['lon_max'], 
        region['lat_max']
    )

def filter_by_fips(gdf, region):
    """Filter GeoDataFrame by state FIPS codes."""
    if 'state_fips' in region:
        return gdf[gdf['STATEFP'].isin(region['state_fips'])]
    elif region.get('is_default', False):
        # For CONUS, exclude all other regions' FIPS codes
        excluded_fips = []
        for r_key, r in REGIONS.items():
            if r_key != 'CONUS' and 'state_fips' in r:
                excluded_fips.extend(r['state_fips'])
        return gdf[~gdf['STATEFP'].isin(excluded_fips)]
    return gdf

def main():
    """Main function to partition counties."""
    print(f"Reading county shapefile from {INPUT_SHAPEFILE}")
    counties = gpd.read_file(INPUT_SHAPEFILE)
    print(f"Loaded {len(counties)} counties")
    
    # Original CRS info
    print(f"Original CRS: {counties.crs}")
    
    # Ensure data is in -180 to 180 longitude format if needed
    if counties.crs != "EPSG:4326" and counties.crs != "EPSG:4269":
        # Convert to WGS84 to ensure -180 to 180 format
        counties = counties.to_crs("EPSG:4326")
        print(f"Converted to WGS84 (EPSG:4326) for consistent longitude format")
    
    # Process each region
    for region_key, region in REGIONS.items():
        print(f"\nProcessing {region['name']}...")
        
        # 1. Filter by FIPS code (primary filtering method)
        region_counties = filter_by_fips(counties, region)
        
        # 2. Create a spatial filter as a backup/validation
        spatial_filter = create_spatial_filter(region)
        spatial_gdf = gpd.GeoDataFrame(geometry=[spatial_filter], crs=counties.crs)
        
        # 3. Check spatial intersection as validation
        spatial_counties = gpd.sjoin(
            counties, 
            spatial_gdf,
            how="inner", 
            predicate="intersects"
        )
        
        # Report any differences (counties in FIPS filter but outside spatial bounds or vice versa)
        fips_ids = set(region_counties['GEOID'])
        spatial_ids = set(spatial_counties['GEOID'])
        
        if fips_ids != spatial_ids:
            fips_only = fips_ids - spatial_ids
            spatial_only = spatial_ids - fips_ids
            print(f"  Warning: {len(fips_only)} counties in FIPS filter but outside spatial bounds")
            print(f"  Warning: {len(spatial_only)} counties in spatial bounds but with different FIPS")
        
        # 4. Export to GeoParquet in original CRS
        output_file = os.path.join(OUTPUT_DIR, f"{region['name'].lower()}_counties.parquet")
        region_counties.to_parquet(output_file)
        print(f"  Saved {len(region_counties)} counties to {output_file}")

if __name__ == "__main__":
    main() 