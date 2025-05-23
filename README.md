# Climate Data Processing Pipeline

A sophisticated climate data processing pipeline for calculating county-level climate statistics from gridded climate model outputs. This project processes daily temperature and precipitation NetCDF files to generate 30-year climate normals and county-level statistics for US territories.

## Overview

The pipeline consists of two main components:

1. **`climate_means.py`** - Preprocesses daily climate data into 30-year climate normals
2. **`county_stats.py`** - Calculates county-level climate statistics using zonal statistics

## Features

### Climate Data Preprocessing (`climate_means.py`)

- **Multi-region support**: CONUS, Alaska, Hawaii, Puerto Rico/USVI, Guam/Northern Mariana Islands
- **30-year moving window climate normals**: Each year's climate based on preceding 30 years
- **Multiple climate scenarios**: Historical (1950-2014) and future projections (SSP126, SSP245, SSP370, SSP585)
- **Coordinate system handling**: Automatic conversion between 0-360° and -180°/180° longitude systems
- **Distributed computing**: Uses Dask for efficient parallel processing
- **Unit conversions**: Kelvin to Celsius for temperature, kg/m²/s to mm/day for precipitation

### County-Level Statistics (`county_stats.py`)

- **Comprehensive climate metrics**:
  - Annual mean temperature
  - Days with maximum temperature > 90°F (32.22°C)
  - Days with minimum temperature < 32°F (0°C)
  - Total annual precipitation
  - Days with precipitation > 1 inch (25.4mm)
- **Advanced performance monitoring**: Real-time tracking of memory usage, processing rates, and parallel efficiency
- **Optimized zonal statistics**: Spatial aggregation from grid cells to county polygons
- **Batch processing**: Efficient handling of large datasets with memory optimization

## Architecture

### Data Flow

```
Raw Climate Data (NetCDF)
         ↓
   climate_means.py
         ↓
Regional Climate Normals (NetCDF)
         ↓
   county_stats.py
         ↓
County-Level Statistics (CSV/GeoPackage)
```

### Regional Coverage

The pipeline supports five US regions with appropriate coordinate reference systems:

- **CONUS**: Continental United States (NAD83 Albers)
- **Alaska**: Alaska (NAD83 Alaska Albers)
- **Hawaii**: Hawaii and Islands (Custom Albers Equal Area)
- **Puerto Rico/USVI**: Puerto Rico and U.S. Virgin Islands (NAD83 Puerto Rico)
- **Guam**: Guam and Northern Mariana Islands (WGS84 UTM Zone 55N)

## Installation

### Using uv (recommended)
```bash
uv pip install -r requirements.txt
```

### Using pip
```bash
pip install -r requirements.txt
```

## Configuration

### External Drive Setup

Update the external drive path in `climate_means.py`:

```python
# Update this path to your external drive mount point
EXTERNAL_DRIVE_PATH = '/Volumes/RPA1TB'  # macOS
# EXTERNAL_DRIVE_PATH = 'D:'             # Windows
# EXTERNAL_DRIVE_PATH = '/media/username/drive'  # Linux
```

### Expected Data Structure

```
/Volumes/RPA1TB/NorESM2-LM/
├── tas/
│   ├── historical/
│   ├── ssp126/
│   ├── ssp245/
│   ├── ssp370/
│   └── ssp585/
├── tasmax/
├── tasmin/
└── pr/
```

## Usage

### Step 1: Process Climate Data

```bash
python climate_means.py
```

This generates regional climate normals in `output/climate_means/`:
- `conus_historical_30yr_climate_normals_1980-2014.nc`
- `alaska_historical_30yr_climate_normals_1980-2014.nc`
- `hawaii_and_islands_historical_30yr_climate_normals_1980-2014.nc`
- `puerto_rico_and_u.s._virgin_islands_historical_30yr_climate_normals_1980-2014.nc`
- `guam_and_northern_mariana_islands_historical_30yr_climate_normals_1980-2014.nc`

### Step 2: Calculate County Statistics

```bash
python county_stats.py
```

This generates county-level statistics in `output/county_climate_stats/`:
- `county_climate_stats.csv` - Climate statistics in CSV format
- `county_climate_stats.gpkg` - Climate statistics with geometries
- `performance_metrics.json` - Detailed performance metrics
- `county_*_daily.parquet` - Daily statistics for each variable

## Performance Monitoring

The pipeline includes comprehensive performance monitoring to track efficiency and identify bottlenecks:

### Features
- **Real-time progress tracking** with progress bars
- **Memory usage monitoring** (peak and average)
- **Dask task monitoring** (concurrent tasks, task states)
- **Stage-by-stage timing** for each processing step
- **Parallel efficiency metrics** (speedup calculations)
- **Automatic performance reporting** with detailed summaries

### Example Performance Output

```
PERFORMANCE SUMMARY
============================================================
Total execution time: 245.67 seconds (4.09 minutes)
Peak memory usage: 12.34 GB
Average memory usage: 8.76 GB
Max concurrent Dask tasks: 24

Stage breakdown:
  load_climate_data: 15.23s (6.2%)
  compute_daily_zonal_stats_tas_2012-2014_mean: 89.45s (36.4%)
  compute_daily_zonal_stats_pr_2012-2014_sum: 67.89s (27.6%)
  create_final_dataset: 12.34s (5.0%)
```

## Unit Conversions

### Temperature: Kelvin to Celsius
```python
T(°C) = T(K) - 273.15
```

**Example**: 295.15 K → 22°C

### Precipitation: Flux to Daily Totals
```python
pr (mm/day) = pr (kg m⁻² s⁻¹) × 86400
```

**Example**: 0.0001 kg m⁻² s⁻¹ → 8.64 mm/day

**Why this works**: 1 kg of water over 1 m² equals 1 mm of water depth. The conversion factor 86400 represents seconds per day.

## Technical Implementation

### Key Optimizations

- **Dask best practices**: Avoided large computation graphs with batch processing
- **Memory efficiency**: Lazy data loading and configurable memory limits
- **Parallel processing**: Optimized task distribution across multiple workers
- **I/O optimization**: Efficient file formats (NetCDF, Parquet) and temporary file management

### Error Handling

- **Path validation**: Automatic detection of available drives and data paths
- **Data validation**: Checks for required variables and coordinate systems
- **Graceful degradation**: Comprehensive logging for debugging

### Climate Methodology

- **30-year normals**: Each year's climate based on preceding 30-year window
- **Spatial aggregation**: Zonal statistics from grid cells to county polygons
- **Threshold calculations**: Climate extreme indices (hot days, cold days, heavy precipitation)

## Output Data

### Climate Normals (NetCDF)
- **Variables**: tas, tasmax, tasmin, pr (temperature and precipitation)
- **Temporal resolution**: Daily climate normals for each target year
- **Spatial resolution**: Original model grid resolution
- **Metadata**: Climate period information, units, methodology

### County Statistics (CSV/GeoPackage)
- **mean_annual_temp**: Annual mean temperature (°C)
- **days_above_90F**: Count of days with max temp > 90°F
- **days_below_32F**: Count of days with min temp < 32°F
- **total_annual_precip**: Total annual precipitation (mm)
- **days_above_1inch_precip**: Count of days with precip > 1 inch

## Dependencies

Core scientific computing stack:
- **xarray**: Multi-dimensional labeled arrays
- **dask**: Parallel computing
- **geopandas**: Geospatial data processing
- **rasterstats**: Zonal statistics
- **netCDF4**: Climate data I/O
- **numpy/pandas**: Numerical computing

See `requirements.txt` for complete dependency list.

## Contributing

This pipeline is designed for climate data analysis workflows. Key areas for enhancement:

1. **Modularization**: Split large scripts into focused modules
2. **Configuration management**: Environment-based configuration
3. **Testing**: Unit tests for core processing functions
4. **Data validation**: Quality checks for input climate data
5. **Error recovery**: Fault-tolerant processing for large datasets

## License

[Add your license information here] 