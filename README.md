# Precipitation Data Conversion

## Converting Precipitation Flux (pr) to Millimeters (mm)

Climate model outputs often provide precipitation as a flux in units of `kg m⁻² s⁻¹` (kilograms per square meter per second). To convert this to millimeters (mm) of precipitation over a given time period, use the following method:

### Why This Works
- 1 kg of water over 1 m² is equivalent to a 1 mm layer of water (since 1 liter = 1 kg = 1 mm over 1 m²).
- The `s⁻¹` (per second) means the value is a rate, so you need to multiply by the number of seconds in your desired time period.

### Formula
For daily precipitation:

```
pr (mm/day) = pr (kg m⁻² s⁻¹) × 86400
```

Where 86400 is the number of seconds in a day (24 × 60 × 60).

### Example
If the model output is `0.0001 kg m⁻² s⁻¹`, then:

```
0.0001 × 86400 = 8.64 mm/day
```

This is the standard approach used in climate science (see CMIP/ERA5/ECMWF documentation). 

---

# Temperature Conversion

## Kelvin to Celsius

To convert temperature from Kelvin (K) to Celsius (°C):

```
T(°C) = T(K) - 273.15
```

**Example:**
If the model output is `295.15 K`, then:
```
295.15 - 273.15 = 22 °C
```

## Kelvin to Fahrenheit

To convert temperature from Kelvin (K) to Fahrenheit (°F):

```
T(°F) = (T(K) - 273.15) × 9/5 + 32
```

**Example:**
If the model output is `295.15 K`, then:
```
(295.15 - 273.15) × 9/5 + 32 = 71.6 °F
``` 

# County Climate Statistics

This project calculates county-level climate statistics from gridded climate data.

## Features

- Annual mean temperature
- Annual count of days with maximum temperature > 90°F (32.22°C)
- Annual count of days with minimum temperature < 32°F (0°C)
- Total annual precipitation
- Annual count of days with precipitation > 1 inch (25.4mm)

## Performance Monitoring

The script includes comprehensive performance monitoring to track efficiency gains and identify bottlenecks:

### Features
- **Real-time progress tracking** with progress bars
- **Memory usage monitoring** (peak and average)
- **Dask task monitoring** (concurrent tasks, task states)
- **Stage-by-stage timing** for each processing step
- **Parallel efficiency metrics** (speedup calculations)
- **Automatic performance reporting** with detailed summaries

### Usage

1. **Run the main script** (performance metrics are automatically collected):
   ```bash
   python county_stats.py
   ```

2. **Performance metrics are saved** to `output/county_climate_stats/performance_metrics.json`

3. **Compare performance between runs**:
   ```bash
   python performance_comparison.py baseline_metrics.json optimized_metrics.json
   ```

### Performance Metrics Collected

- **Total execution time** and stage breakdowns
- **Memory usage**: peak and average RAM consumption
- **Dask efficiency**: task counts, parallel utilization
- **Processing rates**: days/second, parallel speedup
- **File sizes**: input data and output file sizes

### Example Output

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

### Optimization Benefits

The current implementation includes several optimizations based on [Dask best practices](https://docs.dask.org/en/stable/best-practices.html):

- **Avoided large computation graphs** by processing in batches
- **Eliminated embedded large objects** by using file-based data passing
- **Optimized memory usage** with lazy data loading
- **Improved parallelization** with efficient task distribution

## Installation

```bash
# Using uv (recommended)
uv pip install -r requirements.txt

# Or using pip
pip install -r requirements.txt
```

## Usage

```bash
python county_stats.py
```

## Output Files

- `county_climate_stats.csv` - Climate statistics in CSV format
- `county_climate_stats.gpkg` - Climate statistics with geometries
- `performance_metrics.json` - Detailed performance metrics
- `county_*_daily.parquet` - Daily statistics for each variable 