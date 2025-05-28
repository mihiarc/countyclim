#!/usr/bin/env python3
"""
Climate Data Quality Validation Map
Creates interactive maps to visually validate the quality of county climate statistics.
"""

import pandas as pd
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Set up plotting style
plt.style.use('default')
sns.set_palette("viridis")

def load_climate_data(file_path):
    """Load and process climate data."""
    print("Loading climate data...")
    df = pd.read_csv(file_path)
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Extract year and month for analysis
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    
    print(f"Loaded {len(df):,} records")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Counties: {df['county_id'].nunique()}")
    print(f"Climate variable: {[col for col in df.columns if col not in ['county_id', 'date', 'year', 'month']]}")
    
    return df

def load_county_boundaries():
    """Load county boundary data."""
    print("Loading county boundaries...")
    
    # Try to load the contiguous US counties first
    try:
        counties = gpd.read_parquet('output/county_boundaries/contiguous_us_counties.parquet')
        print(f"Loaded {len(counties)} county boundaries")
        
        # Sort counties by state and county FIPS to match the likely order in climate data
        counties = counties.sort_values(['STATEFP', 'COUNTYFP'])
        
        # Create a sequential ID to match the climate data
        counties['county_id'] = range(len(counties))
        
        return counties
    except Exception as e:
        print(f"Error loading parquet file: {e}")
        
        # Fallback to shapefile
        try:
            counties = gpd.read_file('data/tl_2024_us_county/tl_2024_us_county.shp')
            print(f"Loaded {len(counties)} county boundaries from shapefile")
            
            # Sort counties by state and county FIPS to match the likely order in climate data
            counties = counties.sort_values(['STATEFP', 'COUNTYFP'])
            
            # Create a sequential ID to match the climate data
            counties['county_id'] = range(len(counties))
            
            return counties
        except Exception as e:
            print(f"Error loading shapefile: {e}")
            return None

def analyze_data_quality(df):
    """Analyze data quality issues."""
    print("\n=== DATA QUALITY ANALYSIS ===")
    
    # Check for missing values
    missing_data = df.isnull().sum()
    print(f"Missing values:\n{missing_data}")
    
    # Check for negative precipitation (should not exist)
    climate_col = [col for col in df.columns if col not in ['county_id', 'date', 'year', 'month']][0]
    negative_values = (df[climate_col] < 0).sum()
    print(f"Negative values in {climate_col}: {negative_values}")
    
    # Check for extreme outliers
    q99 = df[climate_col].quantile(0.99)
    q01 = df[climate_col].quantile(0.01)
    extreme_high = (df[climate_col] > q99 * 3).sum()
    extreme_low = (df[climate_col] < 0).sum()
    
    print(f"Extreme outliers (>3x 99th percentile): {extreme_high}")
    print(f"Values below 0: {extreme_low}")
    
    # Check data coverage by county
    county_coverage = df.groupby('county_id').size()
    expected_days = (df['date'].max() - df['date'].min()).days + 1
    incomplete_counties = (county_coverage < expected_days * 0.9).sum()
    
    print(f"Counties with <90% data coverage: {incomplete_counties}")
    print(f"Expected days per county: {expected_days}")
    print(f"Actual coverage range: {county_coverage.min()} - {county_coverage.max()} days")
    
    return {
        'climate_col': climate_col,
        'missing_data': missing_data,
        'negative_values': negative_values,
        'extreme_outliers': extreme_high,
        'county_coverage': county_coverage,
        'incomplete_counties': incomplete_counties
    }

def create_county_aggregates(df, climate_col):
    """Create county-level aggregates for mapping."""
    print("Creating county-level aggregates...")
    
    # Calculate various statistics by county
    county_stats = df.groupby('county_id')[climate_col].agg([
        'count',
        'mean',
        'median', 
        'std',
        'min',
        'max',
        lambda x: (x == 0).sum(),  # zero_days
        lambda x: (x < 0).sum(),   # negative_days
    ]).round(3)
    
    county_stats.columns = ['data_count', 'mean_value', 'median_value', 'std_value', 
                           'min_value', 'max_value', 'zero_days', 'negative_days']
    
    # Calculate data completeness
    total_days = df.groupby('county_id').size().max()
    county_stats['data_completeness'] = county_stats['data_count'] / total_days
    
    # Flag potential issues
    county_stats['has_negatives'] = county_stats['negative_days'] > 0
    county_stats['high_zeros'] = county_stats['zero_days'] > (total_days * 0.5)  # >50% zeros
    county_stats['extreme_values'] = county_stats['max_value'] > county_stats['mean_value'] * 10
    
    return county_stats.reset_index()

def create_validation_maps(counties, county_stats, climate_col, output_dir):
    """Create validation maps."""
    print("Creating validation maps...")
    
    # Merge data
    map_data = counties.merge(county_stats, on='county_id', how='left')
    print(f"Mapped data for {map_data['county_id'].notna().sum()} counties out of {len(counties)}")
    
    # Calculate map bounds for contiguous US (excluding Alaska, Hawaii)
    bounds = {
        'minx': -125,  # West coast
        'maxx': -66,   # East coast
        'miny': 24,    # Southern tip of Florida
        'maxy': 50     # Northern border
    }
    
    # Create figure with subplots with extra space at bottom for legends
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    fig.suptitle(f'Climate Data Quality Validation - {climate_col}', fontsize=16, fontweight='bold', y=0.95)
    
    # Helper function for consistent legend formatting
    def add_legend(ax, title, discrete=False):
        if discrete:
            leg = ax.get_legend()
            if leg:
                leg.remove()
        else:
            leg = ax.get_legend()
            if leg:
                leg.set_bbox_to_anchor((0.5, -0.15))
                leg._legend_box.align = "center"
                leg.set_frame_on(True)
                leg.get_frame().set_facecolor('white')
                leg.get_frame().set_alpha(0.8)
    
    # Helper function to set map extent
    def set_map_extent(ax):
        ax.set_xlim([bounds['minx'], bounds['maxx']])
        ax.set_ylim([bounds['miny'], bounds['maxy']])
    
    # 1. Data Completeness
    ax = axes[0, 0]
    map_data.plot(column='data_completeness', ax=ax, legend=True, 
                  cmap='RdYlGn', missing_kwds={'color': 'lightgray'},
                  legend_kwds={'label': 'Completeness',
                              'orientation': 'horizontal',
                              'shrink': 0.8,
                              'aspect': 25,
                              'fraction': 0.1,
                              'pad': 0.2,
                              'location': 'bottom'})
    ax.set_title('Data Completeness\n(1.0 = Complete)')
    ax.axis('off')
    set_map_extent(ax)
    add_legend(ax, 'Completeness')
    
    # 2. Mean Values
    ax = axes[0, 1]
    map_data.plot(column='mean_value', ax=ax, legend=True, 
                  cmap='viridis', missing_kwds={'color': 'lightgray'},
                  legend_kwds={'label': 'Mean (mm)',
                              'orientation': 'horizontal',
                              'shrink': 0.8,
                              'aspect': 25,
                              'fraction': 0.1,
                              'pad': 0.2,
                              'location': 'bottom'})
    ax.set_title(f'Mean {climate_col}')
    ax.axis('off')
    set_map_extent(ax)
    add_legend(ax, 'Mean (mm)')
    
    # 3. Standard Deviation
    ax = axes[0, 2]
    map_data.plot(column='std_value', ax=ax, legend=True, 
                  cmap='plasma', missing_kwds={'color': 'lightgray'},
                  legend_kwds={'label': 'Std Dev',
                              'orientation': 'horizontal',
                              'shrink': 0.8,
                              'aspect': 25,
                              'fraction': 0.1,
                              'pad': 0.2,
                              'location': 'bottom'})
    ax.set_title(f'Standard Deviation')
    ax.axis('off')
    set_map_extent(ax)
    add_legend(ax, 'Std Dev')
    
    # Create custom legends for discrete maps
    from matplotlib.patches import Patch
    
    # 4. Counties with Negative Values
    ax = axes[1, 0]
    map_data['has_negatives_color'] = map_data['has_negatives'].map({True: 'red', False: 'green'})
    map_data.plot(color=map_data['has_negatives_color'].fillna('lightgray'), ax=ax)
    ax.set_title('Counties with Negative Values\n(Red = Has Negatives)')
    ax.axis('off')
    set_map_extent(ax)
    legend_elements = [
        Patch(facecolor='red', label='Has Negatives'),
        Patch(facecolor='green', label='No Negatives'),
        Patch(facecolor='lightgray', label='No Data')
    ]
    ax.legend(handles=legend_elements, loc='center left', bbox_to_anchor=(1, 0.5),
              frameon=True, facecolor='white', framealpha=0.8)
    
    # 5. High Zero Percentage
    ax = axes[1, 1]
    map_data['high_zeros_color'] = map_data['high_zeros'].map({True: 'orange', False: 'blue'})
    map_data.plot(color=map_data['high_zeros_color'].fillna('lightgray'), ax=ax)
    ax.set_title('Counties with >50% Zero Values\n(Orange = High Zeros)')
    ax.axis('off')
    set_map_extent(ax)
    legend_elements = [
        Patch(facecolor='orange', label='>50% Zeros'),
        Patch(facecolor='blue', label='Normal'),
        Patch(facecolor='lightgray', label='No Data')
    ]
    ax.legend(handles=legend_elements, loc='center left', bbox_to_anchor=(1, 0.5),
              frameon=True, facecolor='white', framealpha=0.8)
    
    # 6. Extreme Values
    ax = axes[1, 2]
    map_data['extreme_color'] = map_data['extreme_values'].map({True: 'purple', False: 'cyan'})
    map_data.plot(color=map_data['extreme_color'].fillna('lightgray'), ax=ax)
    ax.set_title('Counties with Extreme Values\n(Purple = Extreme)')
    ax.axis('off')
    set_map_extent(ax)
    legend_elements = [
        Patch(facecolor='purple', label='Extreme Values'),
        Patch(facecolor='cyan', label='Normal Range'),
        Patch(facecolor='lightgray', label='No Data')
    ]
    ax.legend(handles=legend_elements, loc='center left', bbox_to_anchor=(1, 0.5),
              frameon=True, facecolor='white', framealpha=0.8)
    
    # Adjust layout to prevent legend overlap
    plt.tight_layout(rect=[0, 0.05, 1, 0.95])
    
    # Save the map
    output_path = output_dir / f'climate_data_validation_map_{climate_col}.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved validation map: {output_path}")
    
    return map_data

def create_distribution_plots(df, county_stats, climate_col, output_dir):
    """Create distribution plots for additional validation."""
    print("Creating distribution plots...")
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(f'Data Distribution Analysis - {climate_col}', fontsize=14, fontweight='bold')
    
    # 1. Overall value distribution
    ax = axes[0, 0]
    df[climate_col].hist(bins=50, ax=ax, alpha=0.7)
    ax.set_title('Overall Value Distribution')
    ax.set_xlabel(climate_col)
    ax.set_ylabel('Frequency')
    ax.axvline(df[climate_col].mean(), color='red', linestyle='--', label=f'Mean: {df[climate_col].mean():.2f}')
    ax.legend()
    
    # 2. County mean distribution
    ax = axes[0, 1]
    county_stats['mean_value'].hist(bins=30, ax=ax, alpha=0.7)
    ax.set_title('County Mean Distribution')
    ax.set_xlabel(f'Mean {climate_col}')
    ax.set_ylabel('Number of Counties')
    
    # 3. Data completeness distribution
    ax = axes[1, 0]
    county_stats['data_completeness'].hist(bins=20, ax=ax, alpha=0.7)
    ax.set_title('Data Completeness Distribution')
    ax.set_xlabel('Completeness Ratio')
    ax.set_ylabel('Number of Counties')
    
    # 4. Zero days distribution
    ax = axes[1, 1]
    county_stats['zero_days'].hist(bins=30, ax=ax, alpha=0.7)
    ax.set_title('Zero Days Distribution')
    ax.set_xlabel('Number of Zero Days')
    ax.set_ylabel('Number of Counties')
    
    plt.tight_layout()
    
    # Save the plots
    output_path = output_dir / f'climate_data_distributions_{climate_col}.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Saved distribution plots: {output_path}")

def generate_summary_report(df, county_stats, quality_analysis, climate_col, output_dir):
    """Generate a summary report."""
    print("Generating summary report...")
    
    report = []
    report.append("CLIMATE DATA QUALITY VALIDATION REPORT")
    report.append("=" * 50)
    report.append(f"Dataset: county_climate_stats_dask_simple.csv")
    report.append(f"Climate Variable: {climate_col}")
    report.append(f"Analysis Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    report.append("DATASET OVERVIEW:")
    report.append(f"  Total Records: {len(df):,}")
    report.append(f"  Date Range: {df['date'].min()} to {df['date'].max()}")
    report.append(f"  Number of Counties: {df['county_id'].nunique()}")
    report.append(f"  Expected Days per County: {(df['date'].max() - df['date'].min()).days + 1}")
    report.append("")
    
    report.append("DATA QUALITY ISSUES:")
    report.append(f"  Missing Values: {quality_analysis['missing_data'].sum()}")
    report.append(f"  Negative Values: {quality_analysis['negative_values']}")
    report.append(f"  Extreme Outliers: {quality_analysis['extreme_outliers']}")
    report.append(f"  Counties with <90% Coverage: {quality_analysis['incomplete_counties']}")
    report.append("")
    
    report.append("COUNTY-LEVEL STATISTICS:")
    report.append(f"  Counties with Negative Values: {county_stats['has_negatives'].sum()}")
    report.append(f"  Counties with >50% Zeros: {county_stats['high_zeros'].sum()}")
    report.append(f"  Counties with Extreme Values: {county_stats['extreme_values'].sum()}")
    report.append("")
    
    report.append("VALUE STATISTICS:")
    report.append(f"  Overall Mean: {df[climate_col].mean():.3f}")
    report.append(f"  Overall Median: {df[climate_col].median():.3f}")
    report.append(f"  Overall Std Dev: {df[climate_col].std():.3f}")
    report.append(f"  Min Value: {df[climate_col].min():.3f}")
    report.append(f"  Max Value: {df[climate_col].max():.3f}")
    report.append(f"  99th Percentile: {df[climate_col].quantile(0.99):.3f}")
    report.append("")
    
    # Identify problematic counties
    problem_counties = county_stats[
        (county_stats['has_negatives']) | 
        (county_stats['high_zeros']) | 
        (county_stats['extreme_values']) |
        (county_stats['data_completeness'] < 0.9)
    ]
    
    if len(problem_counties) > 0:
        report.append("PROBLEMATIC COUNTIES:")
        for _, county in problem_counties.head(10).iterrows():
            issues = []
            if county['has_negatives']:
                issues.append("negative values")
            if county['high_zeros']:
                issues.append("high zeros")
            if county['extreme_values']:
                issues.append("extreme values")
            if county['data_completeness'] < 0.9:
                issues.append(f"incomplete data ({county['data_completeness']:.1%})")
            
            report.append(f"  County {county['county_id']}: {', '.join(issues)}")
        
        if len(problem_counties) > 10:
            report.append(f"  ... and {len(problem_counties) - 10} more counties with issues")
    else:
        report.append("No major data quality issues detected!")
    
    report.append("")
    report.append("RECOMMENDATIONS:")
    if quality_analysis['negative_values'] > 0:
        report.append("  - Investigate and correct negative precipitation values")
    if quality_analysis['incomplete_counties'] > 0:
        report.append("  - Check data processing for counties with incomplete coverage")
    if county_stats['extreme_values'].sum() > 0:
        report.append("  - Review counties with extreme values for potential errors")
    if county_stats['high_zeros'].sum() > county_stats.shape[0] * 0.1:
        report.append("  - High number of counties with many zero days - verify if expected")
    
    # Save report
    report_text = "\n".join(report)
    report_path = output_dir / f'climate_data_quality_report_{climate_col}.txt'
    with open(report_path, 'w') as f:
        f.write(report_text)
    
    print(f"Saved quality report: {report_path}")
    print("\nSUMMARY:")
    print(report_text)

def main():
    """Main validation function."""
    # Set up paths
    data_file = Path('output/county_climate_stats/county_climate_stats_dask_simple.csv')
    output_dir = Path('output/figures')
    output_dir.mkdir(exist_ok=True)
    
    print("Starting climate data quality validation...")
    
    # Load data
    df = load_climate_data(data_file)
    counties = load_county_boundaries()
    
    if counties is None:
        print("Could not load county boundaries. Exiting.")
        return
    
    # Analyze data quality
    quality_analysis = analyze_data_quality(df)
    climate_col = quality_analysis['climate_col']
    
    # Create county aggregates
    county_stats = create_county_aggregates(df, climate_col)
    
    # Create validation maps
    map_data = create_validation_maps(counties, county_stats, climate_col, output_dir)
    
    # Create distribution plots
    create_distribution_plots(df, county_stats, climate_col, output_dir)
    
    # Generate summary report
    generate_summary_report(df, county_stats, quality_analysis, climate_col, output_dir)
    
    print(f"\nValidation complete! Check {output_dir} for outputs.")

if __name__ == "__main__":
    main() 