#!/usr/bin/env python3
"""
Comprehensive analysis of county climate statistics data
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def load_and_analyze_data():
    """Load and perform comprehensive analysis of the county climate data"""
    
    print("Loading county climate statistics data...")
    df = pd.read_csv('output/county_climate_stats/county_climate_stats_dask_simple.csv')
    
    print(f"\n=== BASIC DATA INFORMATION ===")
    print(f"Dataset shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"Data types:\n{df.dtypes}")
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    print(f"\n=== DATE RANGE ===")
    print(f"Start date: {df['date'].min()}")
    print(f"End date: {df['date'].max()}")
    print(f"Number of days: {(df['date'].max() - df['date'].min()).days + 1}")
    
    print(f"\n=== COUNTY INFORMATION ===")
    print(f"Number of unique counties: {df['county_id'].nunique()}")
    print(f"County ID range: {df['county_id'].min()} to {df['county_id'].max()}")
    
    print(f"\n=== PRECIPITATION STATISTICS (pr_2014) ===")
    pr_stats = df['pr_2014'].describe()
    print(pr_stats)
    
    print(f"\nZero precipitation days: {(df['pr_2014'] == 0).sum()} ({(df['pr_2014'] == 0).mean()*100:.1f}%)")
    print(f"Missing values: {df['pr_2014'].isna().sum()}")
    
    return df

def create_visualizations(df):
    """Create visualizations for the climate data"""
    
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('County Climate Statistics Analysis - Precipitation 2014', fontsize=16, fontweight='bold')
    
    # 1. Distribution of precipitation values
    axes[0, 0].hist(df['pr_2014'], bins=50, alpha=0.7, edgecolor='black')
    axes[0, 0].set_title('Distribution of Daily Precipitation Values')
    axes[0, 0].set_xlabel('Precipitation (mm)')
    axes[0, 0].set_ylabel('Frequency')
    axes[0, 0].set_yscale('log')
    
    # 2. Box plot of precipitation by month
    df['month'] = df['date'].dt.month
    monthly_data = [df[df['month'] == month]['pr_2014'].values for month in range(1, 13)]
    bp = axes[0, 1].boxplot(monthly_data, labels=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
    axes[0, 1].set_title('Monthly Precipitation Distribution')
    axes[0, 1].set_xlabel('Month')
    axes[0, 1].set_ylabel('Precipitation (mm)')
    
    # 3. Time series of average daily precipitation
    daily_avg = df.groupby('date')['pr_2014'].mean()
    axes[1, 0].plot(daily_avg.index, daily_avg.values, alpha=0.7, linewidth=0.8)
    axes[1, 0].set_title('Average Daily Precipitation Across All Counties')
    axes[1, 0].set_xlabel('Date')
    axes[1, 0].set_ylabel('Average Precipitation (mm)')
    axes[1, 0].tick_params(axis='x', rotation=45)
    
    # 4. County-level statistics
    county_stats = df.groupby('county_id')['pr_2014'].agg(['mean', 'std', 'max']).reset_index()
    scatter = axes[1, 1].scatter(county_stats['mean'], county_stats['std'], 
                                alpha=0.6, c=county_stats['max'], cmap='viridis')
    axes[1, 1].set_title('County Precipitation: Mean vs Standard Deviation')
    axes[1, 1].set_xlabel('Mean Precipitation (mm)')
    axes[1, 1].set_ylabel('Standard Deviation (mm)')
    plt.colorbar(scatter, ax=axes[1, 1], label='Max Precipitation (mm)')
    
    plt.tight_layout()
    plt.savefig('output/figures/county_climate_analysis.png', dpi=300, bbox_inches='tight')
    print(f"\nVisualization saved to: output/figures/county_climate_analysis.png")
    
    return county_stats

def analyze_patterns(df):
    """Analyze temporal and spatial patterns in the data"""
    
    print(f"\n=== TEMPORAL PATTERNS ===")
    
    # Monthly averages
    monthly_avg = df.groupby(df['date'].dt.month)['pr_2014'].mean()
    print(f"\nMonthly average precipitation:")
    for month, avg in monthly_avg.items():
        month_name = datetime(2014, month, 1).strftime('%B')
        print(f"  {month_name}: {avg:.2f} mm")
    
    # Seasonal analysis
    def get_season(month):
        if month in [12, 1, 2]:
            return 'Winter'
        elif month in [3, 4, 5]:
            return 'Spring'
        elif month in [6, 7, 8]:
            return 'Summer'
        else:
            return 'Fall'
    
    df['season'] = df['date'].dt.month.apply(get_season)
    seasonal_avg = df.groupby('season')['pr_2014'].mean()
    print(f"\nSeasonal average precipitation:")
    for season in ['Winter', 'Spring', 'Summer', 'Fall']:
        print(f"  {season}: {seasonal_avg[season]:.2f} mm")
    
    print(f"\n=== SPATIAL PATTERNS ===")
    
    # County-level statistics
    county_stats = df.groupby('county_id')['pr_2014'].agg([
        'count', 'mean', 'std', 'min', 'max', 'sum'
    ]).round(2)
    
    print(f"\nCounty-level precipitation statistics (top 10 by mean):")
    top_counties = county_stats.nlargest(10, 'mean')
    print(top_counties)
    
    print(f"\nCounty-level precipitation statistics (bottom 10 by mean):")
    bottom_counties = county_stats.nsmallest(10, 'mean')
    print(bottom_counties)
    
    return county_stats

def extreme_events_analysis(df):
    """Analyze extreme precipitation events"""
    
    print(f"\n=== EXTREME EVENTS ANALYSIS ===")
    
    # Define thresholds
    p95 = df['pr_2014'].quantile(0.95)
    p99 = df['pr_2014'].quantile(0.99)
    
    print(f"95th percentile precipitation: {p95:.2f} mm")
    print(f"99th percentile precipitation: {p99:.2f} mm")
    
    # Extreme events by county
    extreme_events = df[df['pr_2014'] >= p95].groupby('county_id').size()
    print(f"\nCounties with most extreme precipitation events (≥95th percentile):")
    print(extreme_events.nlargest(10))
    
    # Very extreme events
    very_extreme = df[df['pr_2014'] >= p99]
    print(f"\nVery extreme events (≥99th percentile): {len(very_extreme)} events")
    print(f"Counties affected by very extreme events: {very_extreme['county_id'].nunique()}")
    
    # Temporal distribution of extreme events
    extreme_monthly = very_extreme.groupby(very_extreme['date'].dt.month).size()
    print(f"\nVery extreme events by month:")
    for month, count in extreme_monthly.items():
        month_name = datetime(2014, month, 1).strftime('%B')
        print(f"  {month_name}: {count} events")

def data_quality_check(df):
    """Perform data quality checks"""
    
    print(f"\n=== DATA QUALITY ASSESSMENT ===")
    
    # Check for missing values
    missing_data = df.isnull().sum()
    print(f"Missing values per column:")
    for col, missing in missing_data.items():
        print(f"  {col}: {missing} ({missing/len(df)*100:.2f}%)")
    
    # Check for negative precipitation (should not exist)
    negative_pr = (df['pr_2014'] < 0).sum()
    print(f"\nNegative precipitation values: {negative_pr}")
    
    # Check data completeness by county
    expected_days = 365  # 2014 is not a leap year
    county_counts = df.groupby('county_id').size()
    incomplete_counties = county_counts[county_counts != expected_days]
    
    print(f"\nData completeness check:")
    print(f"Expected days per county: {expected_days}")
    print(f"Counties with incomplete data: {len(incomplete_counties)}")
    
    if len(incomplete_counties) > 0:
        print(f"Counties with missing days:")
        for county_id, days in incomplete_counties.items():
            print(f"  County {county_id}: {days} days (missing {expected_days - days})")

def main():
    """Main analysis function"""
    
    print("=== COUNTY CLIMATE STATISTICS ANALYSIS ===")
    print(f"Analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load and analyze data
    df = load_and_analyze_data()
    
    # Perform data quality checks
    data_quality_check(df)
    
    # Analyze patterns
    county_stats = analyze_patterns(df)
    
    # Analyze extreme events
    extreme_events_analysis(df)
    
    # Create visualizations
    county_stats_viz = create_visualizations(df)
    
    # Save summary statistics
    print(f"\n=== SAVING RESULTS ===")
    county_stats.to_csv('output/county_climate_stats/county_summary_statistics.csv')
    print(f"County summary statistics saved to: output/county_climate_stats/county_summary_statistics.csv")
    
    print(f"\nAnalysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main() 