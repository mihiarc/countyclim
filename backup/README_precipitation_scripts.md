# Precipitation Validation Scripts

This directory contains scripts for quickly validating precipitation processing before running the full county climate statistics computation.

## Scripts Overview

### 1. `run_precipitation_tests.py` (START HERE)
**Main runner script** - executes the full validation workflow in the correct order.

```bash
python run_precipitation_tests.py
```

This script will:
1. Run a quick test with 5 counties (~1-2 minutes)
2. Run a partial analysis with 100 counties (~5-10 minutes) 
3. Validate the results and provide summary statistics

### 2. `test_precipitation_quick.py`
**Quick validation** - processes just 5 counties to verify precipitation calculations work correctly.

- **Purpose**: Fast verification that the processing pipeline works
- **Runtime**: ~1-2 minutes
- **Output**: `output/test_precip_quick/quick_test_results.csv`

### 3. `county_stats_partial.py`
**Partial analysis** - processes 100 counties with precipitation variables only.

- **Purpose**: Generate enough data to validate precipitation statistics
- **Runtime**: ~5-10 minutes
- **Variables**: Only precipitation (total annual, days >1 inch)
- **Output**: `output/county_climate_stats_partial/`

### 4. `validate_partial_results.py`
**Results validation** - analyzes the partial results for data quality and reasonableness.

- **Purpose**: Check if precipitation values are realistic
- **Checks**: Data completeness, value ranges, geographic patterns
- **Output**: Console summary with validation results

## Expected Results

### Precipitation Values (Typical US Ranges)
- **Annual precipitation**: 10-60 inches (254-1524 mm)
- **Heavy precipitation days**: 0-20 days per year
- **Daily precipitation**: 0-200 mm (0-8 inches)

### Data Quality Indicators
- ✅ **Good**: >90% of counties have complete data
- ⚠️ **Acceptable**: 70-90% of counties have complete data  
- ❌ **Poor**: <70% of counties have complete data

## Troubleshooting

### Common Issues

1. **Missing input files**
   ```
   Error: Missing required files
   ```
   **Solution**: Run the data preparation scripts first:
   ```bash
   python climate_means.py
   python partition_counties.py
   ```

2. **Memory errors**
   ```
   Error: Out of memory
   ```
   **Solution**: The scripts are configured for 16GB RAM. Reduce `SUBSET_SIZE` in the scripts if needed.

3. **Unrealistic precipitation values**
   ```
   Warning: Some precipitation values may be unrealistic
   ```
   **Solution**: Check the climate data source and coordinate system alignment.

## File Outputs

### Quick Test (`output/test_precip_quick/`)
- `quick_test_results.csv` - Results for 5 test counties

### Partial Run (`output/county_climate_stats_partial/`)
- `county_pr_2012-2014_sum_daily.parquet` - Daily precipitation totals
- `county_pr_2012-2014_threshold_daily.parquet` - Daily heavy precipitation flags
- `county_climate_stats_partial.csv` - Final annual statistics (CSV)
- `county_climate_stats_partial.gpkg` - Final annual statistics (GeoPackage)

## Next Steps

After successful validation:

1. **Review the results** in the validation output
2. **Check sample values** to ensure they match your expectations
3. **Run the full analysis** with `python county_stats.py`

## Configuration

### Adjustable Parameters

In `county_stats_partial.py`:
- `SUBSET_SIZE = 100` - Number of counties to process
- `MEMORY_LIMIT = '16GB'` - Dask memory limit
- `batch_size = 15` - Days per processing batch

In `test_precipitation_quick.py`:
- `subset_size=5` - Number of counties for quick test

## Performance Notes

- **Quick test**: Uses 2 workers, 4GB memory
- **Partial run**: Uses all CPU cores - 1, 16GB memory
- **Batch processing**: 15-day batches for faster feedback
- **Pre-computation**: Data loaded immediately for development speed

## Validation Criteria

The scripts check for:
- ✅ Reasonable annual precipitation (10-60 inches)
- ✅ Reasonable heavy precipitation days (0-20 per year)
- ✅ Complete data coverage for processed counties
- ✅ No extreme outliers or impossible values
- ✅ Consistent geographic patterns 