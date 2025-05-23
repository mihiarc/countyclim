#!/usr/bin/env python3
"""
create_county_masks.py

Standalone script to create spatial masks for counties using regionmask.
"""
import argparse
import logging
import os
import xarray as xr
import geopandas as gpd
import regionmask
import numpy as np
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_counties(counties_path, county_id_col='county_id'):
    """
    Load county boundary data from file.
    
    Args:
        counties_path (str): Path to the county boundaries file (Shapefile, GeoJSON, parquet, etc.)
        county_id_col (str): County ID column name
        
    Returns:
        GeoDataFrame: County boundaries
    """
    logger.info(f"Loading county boundaries from {counties_path}")
    
    # Check file extension to determine loading method
    file_ext = os.path.splitext(counties_path)[1].lower()
    
    if file_ext == '.parquet':
        # Load parquet file
        try:
            counties = gpd.read_parquet(counties_path)
        except Exception as e:
            logger.warning(f"Error reading with gpd.read_parquet: {e}")
            logger.info("Trying to read with pandas and convert to GeoDataFrame...")
            counties_df = pd.read_parquet(counties_path)
            counties = gpd.GeoDataFrame(counties_df)
    else:
        # Load other formats (shapefile, geojson, etc.)
        counties = gpd.read_file(counties_path)
    
    # Print info about CRS
    logger.info(f"Counties CRS: {counties.crs}")
    
    # Ensure county_id_col exists, create if not
    if county_id_col not in counties.columns:
        logger.warning(f"Column {county_id_col} not found, using GEOID as county ID")
        # Try GEOID (common for US Census data) or index as fallback
        if 'GEOID' in counties.columns:
            counties[county_id_col] = counties['GEOID']
        else:
            logger.warning("GEOID not found, using index as county ID")
            counties[county_id_col] = counties.index
    
    # Create a new unique numeric ID for each county
    counties['id_numeric'] = np.arange(1, len(counties) + 1)
    
    # Print state distribution
    if 'STATEFP' in counties.columns:
        state_counts = counties.groupby('STATEFP').size()
        logger.info(f"Counties by state (top 5): {state_counts.sort_values(ascending=False).head()}")
    
    return counties

def create_mask(ds, counties, lon_coord, lat_coord, county_id_col='county_id'):
    """
    Create county mask using regionmask.
    
    Args:
        ds (xarray.Dataset): The dataset with coordinates to mask
        counties (GeoDataFrame): County boundaries
        lon_coord (str): Longitude coordinate name
        lat_coord (str): Latitude coordinate name
        county_id_col (str): County ID column name
        
    Returns:
        xarray.DataArray: Mask with county IDs
    """
    logger.info("Creating county mask with regionmask...")
    counties = counties.copy()
    
    # Print bounds of first county
    if not counties.empty:
        logger.info(f"First county bounds: {counties.geometry.iloc[0].bounds}")
        logger.info(f"Counties total bounds: {counties.total_bounds}")
    
    # Log county info for debugging
    logger.info(f"Number of counties: {len(counties)}")
    logger.info(f"Sample of counties being processed:")
    for state_fp in sorted(counties.STATEFP.unique())[:5]:  # Sample from first 5 states
        state_counties = counties[counties.STATEFP == state_fp].head(2)
        for _, county in state_counties.iterrows():
            logger.info(f"  State {state_fp}, County {county.NAME}, ID: {county.id_numeric}")
    
    # Create regions using simple sequential IDs
    county_regions = regionmask.from_geopandas(
        counties,
        names=county_id_col,
        numbers='id_numeric',
        name="counties"
    )
    
    # Create mask
    mask = county_regions.mask(ds[lon_coord], ds[lat_coord])
    
    # Check if mask contains any non-NaN values
    non_nan_count = mask.notnull().sum().values
    logger.info(f"Non-NaN values in mask: {non_nan_count}")
    if non_nan_count == 0:
        logger.warning("Mask contains only NaN values - check coordinate systems match")
    
    # Check which counties were masked
    masked_ids = np.unique(mask.values)
    masked_ids = masked_ids[~np.isnan(masked_ids)]
    logger.info(f"Number of unique county IDs in mask: {len(masked_ids)}")
    if len(masked_ids) > 0:
        logger.info(f"Sample of masked county IDs: {sorted(masked_ids.astype(int))[:10]}")
        
        # Check which states are represented in the masked counties
        if 'STATEFP' in counties.columns:
            masked_counties = counties[counties.id_numeric.isin(masked_ids)]
            masked_states = masked_counties.STATEFP.unique()
            logger.info(f"States with masked counties: {sorted(masked_states)[:10]}")
    
    return mask

def main():
    # Hardcoded paths
    counties_path = "data/tl_2024_us_county/tl_2024_us_county.shp"
    climate_data_path = "data/tas/tas_day_NorESM2-LM_historical_r1i1p1f1_gn_2012.nc"
    output_path = "output/county_masks/county_mask.nc"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    lon_coord = "lon"
    lat_coord = "lat"
    county_id = "GEOID"
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Parse command line arguments for optional overrides
    parser = argparse.ArgumentParser(description='Create county masks using regionmask')
    parser.add_argument('--counties', default=counties_path, help='Path to county boundaries file')
    parser.add_argument('--climate-data', default=climate_data_path, help='Path to climate data file with coordinates')
    parser.add_argument('--output', '-o', default=output_path, help='Output file path')
    parser.add_argument('--lon', default=lon_coord, help='Longitude coordinate name')
    parser.add_argument('--lat', default=lat_coord, help='Latitude coordinate name')
    parser.add_argument('--county-id', default=county_id, help='County ID column name')
    
    args = parser.parse_args()
    
    # Load county boundaries
    counties = load_counties(args.counties, args.county_id)
    
    # Load climate data (only need coordinates)
    logger.info(f"Loading coordinates from {args.climate_data}")
    ds = xr.open_dataset(args.climate_data)
    
    # Create mask
    mask = create_mask(ds, counties, args.lon, args.lat, args.county_id)
    
    # Save mask
    logger.info(f"Saving county mask to {args.output}")
    mask.to_netcdf(args.output)
    logger.info("Done!")

if __name__ == "__main__":
    main() 