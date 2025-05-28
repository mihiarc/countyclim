#!/usr/bin/env python3
"""Quick validation script to compare output files."""

import pandas as pd
import numpy as np
import os

def main():
    # Load both files
    mp_file = 'output/county_climate_stats/county_climate_stats.csv'
    dask_file = 'output/county_climate_stats/county_climate_stats_dask_simple.csv'

    print('Loading files...')
    df_mp = pd.read_csv(mp_file)
    df_dask = pd.read_csv(dask_file)

    print(f'Multiprocessing output shape: {df_mp.shape}')
    print(f'Dask output shape: {df_dask.shape}')

    print(f'Multiprocessing columns: {list(df_mp.columns)}')
    print(f'Dask columns: {list(df_dask.columns)}')

    print(f'Multiprocessing file size: {os.path.getsize(mp_file) / 1024**2:.2f} MB')
    print(f'Dask file size: {os.path.getsize(dask_file) / 1024**2:.2f} MB')

    # Check first few rows
    print('\nFirst 5 rows of multiprocessing output:')
    print(df_mp.head())

    print('\nFirst 5 rows of Dask output:')
    print(df_dask.head())

    # Check if shapes match
    if df_mp.shape == df_dask.shape:
        print('\n✅ Shapes match!')
        
        # Sort both for comparison
        df_mp_sorted = df_mp.sort_values(['county_id', 'date']).reset_index(drop=True)
        df_dask_sorted = df_dask.sort_values(['county_id', 'date']).reset_index(drop=True)
        
        # Compare values
        try:
            pd.testing.assert_frame_equal(df_mp_sorted, df_dask_sorted, check_dtype=False, rtol=1e-10, atol=1e-10)
            print('✅ Data values are identical!')
        except AssertionError as e:
            print(f'⚠️ Data differences found: {str(e)[:200]}...')
            
            # Check numeric columns for differences
            numeric_cols = df_mp_sorted.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                if col in df_dask_sorted.columns:
                    diff = np.abs(df_mp_sorted[col] - df_dask_sorted[col])
                    max_diff = diff.max()
                    mean_diff = diff.mean()
                    print(f'Column {col}: max_diff={max_diff:.2e}, mean_diff={mean_diff:.2e}')
    else:
        print('❌ Shapes do not match!')

if __name__ == '__main__':
    main() 