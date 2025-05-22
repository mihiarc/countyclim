#!/usr/bin/env python3
"""
Validation script for county-level climate statistics.

This script validates the output from county_climate_stats.py by:
1. Creating choropleth maps of climate statistics
2. Generating summary statistics by state
3. Checking for outliers and suspicious patterns
4. Creating validation plots for visual inspection
"""

import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import logging
from matplotlib.colors import LinearSegmentedColormap
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define paths
INPUT_FILE = "output/county_climate_stats/county_climate_stats.gpkg"
OUTPUT_DIR = "output/county_climate_stats/validation"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Define expected value ranges for each variable
EXPECTED_RANGES = {
    'mean_annual_temp': (-5, 25),  # °C
    'days_above_90F': (0, 180),    # days
    'days_below_32F': (0, 250),    # days
    'total_annual_precip': (0, 2000),  # mm
    'days_above_1inch_precip': (0, 50)  # days
}

def load_and_validate_data():
    """Load county statistics and perform basic validation."""
    logger.info("Loading county statistics from %s", INPUT_FILE)
    df = gpd.read_file(INPUT_FILE)
    
    # Basic data validation
    logger.info("Performing basic data validation")
    logger.info("Number of counties: %d", len(df))
    
    # Check for missing values
    missing = df.isnull().sum()
    if missing.any():
        logger.warning("Missing values found:")
        for col, count in missing[missing > 0].items():
            logger.warning("  %s: %d missing values", col, count)
    
    # Check for values outside expected ranges
    for var, (min_val, max_val) in EXPECTED_RANGES.items():
        outliers = df[df[var].notnull() & ((df[var] < min_val) | (df[var] > max_val))]
        if len(outliers) > 0:
            logger.warning("Outliers found in %s:", var)
            logger.warning("  Min: %.2f, Max: %.2f", df[var].min(), df[var].max())
            logger.warning("  Counties with outliers:")
            for _, row in outliers.iterrows():
                logger.warning("    %s, %s: %.2f", row['NAME'], row['STUSPS'], row[var])
    
    return df

def plot_choropleth(df, variable, title, output_file, cmap='RdBu_r'):
    """Create a choropleth map for a given variable."""
    fig = plt.figure(figsize=(15, 10))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.AlbersEqualArea(
        central_longitude=-96, central_latitude=38))
    
    # Plot counties
    df.plot(column=variable, 
           cmap=cmap,
           linewidth=0.1,
           edgecolor='0.5',
           ax=ax,
           transform=ccrs.PlateCarree(),
           legend=True,
           legend_kwds={'label': title, 'orientation': 'horizontal'})
    
    # Add state boundaries and coastlines
    ax.add_feature(cfeature.STATES, linewidth=0.5)
    ax.add_feature(cfeature.COASTLINE)
    
    # Set extent to CONUS
    ax.set_extent([-125, -66.5, 24, 49], crs=ccrs.PlateCarree())
    
    plt.title(title)
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()

def create_state_summary(df):
    """Create summary statistics by state."""
    logger.info("Generating state-level summary statistics")
    
    # Group by state and calculate statistics
    state_stats = df.groupby('STUSPS').agg({
        'mean_annual_temp': ['mean', 'min', 'max', 'std'],
        'days_above_90F': ['mean', 'min', 'max', 'std'],
        'days_below_32F': ['mean', 'min', 'max', 'std'],
        'total_annual_precip': ['mean', 'min', 'max', 'std'],
        'days_above_1inch_precip': ['mean', 'min', 'max', 'std']
    }).round(2)
    
    # Save state summary to CSV
    output_file = os.path.join(OUTPUT_DIR, 'state_summary_statistics.csv')
    state_stats.to_csv(output_file)
    logger.info("Saved state summary to %s", output_file)
    
    return state_stats

def plot_correlation_matrix(df):
    """Create a correlation matrix plot for climate variables."""
    climate_vars = ['mean_annual_temp', 'days_above_90F', 'days_below_32F', 
                   'total_annual_precip', 'days_above_1inch_precip']
    
    corr = df[climate_vars].corr()
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr, annot=True, cmap='RdBu_r', center=0, vmin=-1, vmax=1)
    plt.title('Correlation Matrix of Climate Variables')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'correlation_matrix.png'))
    plt.close()

def plot_climate_relationships(df):
    """Create scatter plots showing relationships between climate variables."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 15))
    fig.suptitle('Climate Variable Relationships', fontsize=16)
    
    # Temperature vs Frost Days
    ax = axes[0, 0]
    ax.scatter(df['mean_annual_temp'], df['days_below_32F'], alpha=0.5)
    ax.set_xlabel('Mean Annual Temperature (°C)')
    ax.set_ylabel('Days Below 32°F')
    ax.set_title('Temperature vs Frost Days')
    
    # Temperature vs Hot Days
    ax = axes[0, 1]
    ax.scatter(df['mean_annual_temp'], df['days_above_90F'], alpha=0.5)
    ax.set_xlabel('Mean Annual Temperature (°C)')
    ax.set_ylabel('Days Above 90°F')
    ax.set_title('Temperature vs Hot Days')
    
    # Precipitation vs Hot Days
    ax = axes[1, 0]
    ax.scatter(df['total_annual_precip'], df['days_above_90F'], alpha=0.5)
    ax.set_xlabel('Total Annual Precipitation (mm)')
    ax.set_ylabel('Days Above 90°F')
    ax.set_title('Precipitation vs Hot Days')
    
    # Precipitation vs Heavy Precipitation Days
    ax = axes[1, 1]
    ax.scatter(df['total_annual_precip'], df['days_above_1inch_precip'], alpha=0.5)
    ax.set_xlabel('Total Annual Precipitation (mm)')
    ax.set_ylabel('Days with >1 inch Precipitation')
    ax.set_title('Total vs Heavy Precipitation Days')
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'climate_relationships.png'))
    plt.close()

def create_validation_report(df, state_stats):
    """Create a text report with validation findings."""
    report_file = os.path.join(OUTPUT_DIR, 'validation_report.txt')
    
    with open(report_file, 'w') as f:
        f.write("County Climate Statistics Validation Report\n")
        f.write("========================================\n\n")
        
        # Overall statistics
        f.write("Overall Statistics:\n")
        f.write("-----------------\n")
        for var in EXPECTED_RANGES.keys():
            stats = df[var].describe()
            f.write(f"\n{var}:\n")
            f.write(f"  Mean: {stats['mean']:.2f}\n")
            f.write(f"  Std: {stats['std']:.2f}\n")
            f.write(f"  Min: {stats['min']:.2f}\n")
            f.write(f"  Max: {stats['max']:.2f}\n")
        
        # Correlation analysis
        f.write("\nKey Correlations:\n")
        f.write("---------------\n")
        corr = df[list(EXPECTED_RANGES.keys())].corr()
        for var1 in EXPECTED_RANGES.keys():
            for var2 in EXPECTED_RANGES.keys():
                if var1 < var2:  # Only print each correlation once
                    correlation = corr.loc[var1, var2]
                    if abs(correlation) > 0.5:  # Only print strong correlations
                        f.write(f"{var1} vs {var2}: {correlation:.3f}\n")
        
        # Extreme values
        f.write("\nExtreme Values:\n")
        f.write("--------------\n")
        for var in EXPECTED_RANGES.keys():
            f.write(f"\n{var}:\n")
            # Top 5 highest
            f.write("  Highest values:\n")
            top_5 = df.nlargest(5, var)
            for _, row in top_5.iterrows():
                f.write(f"    {row['NAME']}, {row['STUSPS']}: {row[var]:.2f}\n")
            # Top 5 lowest
            f.write("  Lowest values:\n")
            bottom_5 = df.nsmallest(5, var)
            for _, row in bottom_5.iterrows():
                f.write(f"    {row['NAME']}, {row['STUSPS']}: {row[var]:.2f}\n")

def main():
    """Main validation function."""
    logger.info("Starting county climate statistics validation")
    
    # Load and validate data
    df = load_and_validate_data()
    
    # Create choropleth maps
    logger.info("Creating choropleth maps")
    plot_choropleth(df, 'mean_annual_temp', 'Mean Annual Temperature (°C)', 
                   os.path.join(OUTPUT_DIR, 'mean_annual_temp.png'))
    plot_choropleth(df, 'days_above_90F', 'Days Above 90°F', 
                   os.path.join(OUTPUT_DIR, 'days_above_90F.png'), 'YlOrRd')
    plot_choropleth(df, 'days_below_32F', 'Days Below 32°F', 
                   os.path.join(OUTPUT_DIR, 'days_below_32F.png'), 'YlOrRd')
    plot_choropleth(df, 'total_annual_precip', 'Total Annual Precipitation (mm)', 
                   os.path.join(OUTPUT_DIR, 'total_annual_precip.png'), 'YlGnBu')
    plot_choropleth(df, 'days_above_1inch_precip', 'Days with >1 inch Precipitation', 
                   os.path.join(OUTPUT_DIR, 'days_above_1inch_precip.png'), 'YlGnBu')
    
    # Create state summary
    state_stats = create_state_summary(df)
    
    # Create correlation matrix
    logger.info("Creating correlation matrix")
    plot_correlation_matrix(df)
    
    # Create climate relationship plots
    logger.info("Creating climate relationship plots")
    plot_climate_relationships(df)
    
    # Create validation report
    logger.info("Creating validation report")
    create_validation_report(df, state_stats)
    
    logger.info("Validation complete. Results saved in: %s", OUTPUT_DIR)

if __name__ == "__main__":
    main() 