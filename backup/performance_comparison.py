#!/usr/bin/env python3
"""
performance_comparison.py

Comprehensive performance comparison between county_stats.py (multiprocessing) 
and county_stats_dask.py (Dask distributed) implementations.

This script:
1. Tests both implementations on the same real climate data
2. Measures execution time, memory usage, and CPU utilization
3. Validates that both produce identical results
4. Generates detailed performance reports and visualizations
"""

import os
import sys
import time
import psutil
import subprocess
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from pathlib import Path
from datetime import datetime
import json
import threading
import warnings
from typing import Dict, List, Tuple, Optional
import hashlib

warnings.filterwarnings('ignore')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'performance_comparison_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

class SystemMonitor:
    """Monitor system resources during script execution."""
    
    def __init__(self, interval: float = 1.0):
        self.interval = interval
        self.monitoring = False
        self.data = []
        self.thread = None
        
    def start(self):
        """Start monitoring system resources."""
        self.monitoring = True
        self.data = []
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.thread.start()
        logger.info("System monitoring started")
        
    def stop(self):
        """Stop monitoring and return collected data."""
        self.monitoring = False
        if self.thread:
            self.thread.join(timeout=2)
        logger.info(f"System monitoring stopped. Collected {len(self.data)} data points")
        return self.data.copy()
        
    def _monitor(self):
        """Background monitoring loop."""
        start_time = time.time()
        while self.monitoring:
            try:
                # Get system metrics
                cpu_percent = psutil.cpu_percent(interval=None)
                memory = psutil.virtual_memory()
                disk_io = psutil.disk_io_counters()
                net_io = psutil.net_io_counters()
                
                # Get process-specific metrics if available
                try:
                    process = psutil.Process()
                    process_memory = process.memory_info().rss / 1024**3  # GB
                    process_cpu = process.cpu_percent()
                except:
                    process_memory = 0
                    process_cpu = 0
                
                self.data.append({
                    'timestamp': time.time() - start_time,
                    'cpu_percent': cpu_percent,
                    'memory_used_gb': memory.used / 1024**3,
                    'memory_percent': memory.percent,
                    'memory_available_gb': memory.available / 1024**3,
                    'process_memory_gb': process_memory,
                    'process_cpu_percent': process_cpu,
                    'disk_read_mb': disk_io.read_bytes / 1024**2 if disk_io else 0,
                    'disk_write_mb': disk_io.write_bytes / 1024**2 if disk_io else 0,
                    'net_sent_mb': net_io.bytes_sent / 1024**2 if net_io else 0,
                    'net_recv_mb': net_io.bytes_recv / 1024**2 if net_io else 0,
                })
                
                time.sleep(self.interval)
            except Exception as e:
                logger.warning(f"Monitoring error: {e}")
                time.sleep(self.interval)

class PerformanceTest:
    """Class to run and measure performance of climate processing scripts."""
    
    def __init__(self, test_name: str):
        self.test_name = test_name
        self.results = {}
        self.system_monitor = SystemMonitor(interval=0.5)  # Monitor every 0.5 seconds
        
    def run_script(self, script_path: str, script_args: List[str] = None) -> Dict:
        """Run a script and measure its performance."""
        logger.info(f"Running {script_path} for {self.test_name}")
        
        if script_args is None:
            script_args = []
            
        # Start system monitoring
        self.system_monitor.start()
        start_time = time.time()
        
        try:
            # Run the script
            cmd = [sys.executable, script_path] + script_args
            logger.info(f"Executing command: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=os.getcwd(),
                timeout=3600  # 1 hour timeout
            )
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Stop monitoring and get data
            monitoring_data = self.system_monitor.stop()
            
            # Parse performance metrics from monitoring data
            if monitoring_data:
                cpu_usage = [d['cpu_percent'] for d in monitoring_data]
                memory_usage = [d['memory_used_gb'] for d in monitoring_data]
                process_memory = [d['process_memory_gb'] for d in monitoring_data]
                
                performance_metrics = {
                    'execution_time_seconds': execution_time,
                    'execution_time_minutes': execution_time / 60,
                    'peak_cpu_percent': max(cpu_usage) if cpu_usage else 0,
                    'avg_cpu_percent': np.mean(cpu_usage) if cpu_usage else 0,
                    'peak_memory_gb': max(memory_usage) if memory_usage else 0,
                    'avg_memory_gb': np.mean(memory_usage) if memory_usage else 0,
                    'peak_process_memory_gb': max(process_memory) if process_memory else 0,
                    'avg_process_memory_gb': np.mean(process_memory) if process_memory else 0,
                    'monitoring_data': monitoring_data
                }
            else:
                performance_metrics = {
                    'execution_time_seconds': execution_time,
                    'execution_time_minutes': execution_time / 60,
                    'peak_cpu_percent': 0,
                    'avg_cpu_percent': 0,
                    'peak_memory_gb': 0,
                    'avg_memory_gb': 0,
                    'peak_process_memory_gb': 0,
                    'avg_process_memory_gb': 0,
                    'monitoring_data': []
                }
            
            # Add script execution results
            performance_metrics.update({
                'return_code': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'success': result.returncode == 0
            })
            
            if result.returncode == 0:
                logger.info(f"✅ {script_path} completed successfully in {execution_time:.2f} seconds")
            else:
                logger.error(f"❌ {script_path} failed with return code {result.returncode}")
                logger.error(f"STDERR: {result.stderr}")
                
            return performance_metrics
            
        except subprocess.TimeoutExpired:
            self.system_monitor.stop()
            logger.error(f"❌ {script_path} timed out after 1 hour")
            return {
                'execution_time_seconds': 3600,
                'execution_time_minutes': 60,
                'return_code': -1,
                'success': False,
                'error': 'Timeout',
                'monitoring_data': []
            }
        except Exception as e:
            self.system_monitor.stop()
            logger.error(f"❌ Error running {script_path}: {str(e)}")
            return {
                'execution_time_seconds': 0,
                'execution_time_minutes': 0,
                'return_code': -1,
                'success': False,
                'error': str(e),
                'monitoring_data': []
            }

def validate_output_files(multiprocessing_output: str, dask_output: str) -> Dict:
    """Validate that both scripts produce identical results."""
    logger.info("Validating output files...")
    
    validation_results = {
        'files_exist': False,
        'identical_results': False,
        'row_count_match': False,
        'column_count_match': False,
        'data_hash_match': False,
        'details': {}
    }
    
    try:
        # Check if both files exist
        mp_exists = os.path.exists(multiprocessing_output)
        dask_exists = os.path.exists(dask_output)
        
        validation_results['files_exist'] = mp_exists and dask_exists
        validation_results['details']['multiprocessing_file_exists'] = mp_exists
        validation_results['details']['dask_file_exists'] = dask_exists
        
        if not validation_results['files_exist']:
            logger.warning(f"Missing output files - MP: {mp_exists}, Dask: {dask_exists}")
            return validation_results
        
        # Load both datasets
        logger.info("Loading output files for comparison...")
        df_mp = pd.read_csv(multiprocessing_output)
        df_dask = pd.read_csv(dask_output)
        
        # Basic shape comparison
        validation_results['details']['multiprocessing_shape'] = df_mp.shape
        validation_results['details']['dask_shape'] = df_dask.shape
        validation_results['row_count_match'] = df_mp.shape[0] == df_dask.shape[0]
        validation_results['column_count_match'] = df_mp.shape[1] == df_dask.shape[1]
        
        logger.info(f"Multiprocessing output shape: {df_mp.shape}")
        logger.info(f"Dask output shape: {df_dask.shape}")
        
        if validation_results['row_count_match'] and validation_results['column_count_match']:
            # Sort both dataframes for comparison
            df_mp_sorted = df_mp.sort_values(['county_id', 'date']).reset_index(drop=True)
            df_dask_sorted = df_dask.sort_values(['county_id', 'date']).reset_index(drop=True)
            
            # Compare data values (allowing for small numerical differences)
            try:
                # Check if dataframes are approximately equal
                pd.testing.assert_frame_equal(
                    df_mp_sorted, 
                    df_dask_sorted, 
                    check_dtype=False,
                    rtol=1e-10,  # Relative tolerance
                    atol=1e-10   # Absolute tolerance
                )
                validation_results['identical_results'] = True
                logger.info("✅ Output files are identical!")
                
            except AssertionError as e:
                logger.warning(f"⚠️ Output files have differences: {str(e)}")
                
                # Calculate hash for each dataset to check if they're exactly the same
                mp_hash = hashlib.md5(df_mp_sorted.to_string().encode()).hexdigest()
                dask_hash = hashlib.md5(df_dask_sorted.to_string().encode()).hexdigest()
                validation_results['data_hash_match'] = mp_hash == dask_hash
                
                # Detailed comparison for numeric columns
                numeric_cols = df_mp_sorted.select_dtypes(include=[np.number]).columns
                for col in numeric_cols:
                    if col in df_dask_sorted.columns:
                        diff = np.abs(df_mp_sorted[col] - df_dask_sorted[col])
                        max_diff = diff.max()
                        mean_diff = diff.mean()
                        validation_results['details'][f'{col}_max_diff'] = max_diff
                        validation_results['details'][f'{col}_mean_diff'] = mean_diff
                        logger.info(f"Column {col}: max_diff={max_diff:.2e}, mean_diff={mean_diff:.2e}")
        
        # File size comparison
        mp_size = os.path.getsize(multiprocessing_output) / 1024**2  # MB
        dask_size = os.path.getsize(dask_output) / 1024**2  # MB
        validation_results['details']['multiprocessing_file_size_mb'] = mp_size
        validation_results['details']['dask_file_size_mb'] = dask_size
        
        logger.info(f"File sizes - MP: {mp_size:.2f} MB, Dask: {dask_size:.2f} MB")
        
    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        validation_results['details']['validation_error'] = str(e)
    
    return validation_results

def create_performance_visualizations(results: Dict, output_dir: str):
    """Create performance comparison visualizations."""
    logger.info("Creating performance visualizations...")
    
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    # Create output directory
    viz_dir = Path(output_dir) / 'visualizations'
    viz_dir.mkdir(parents=True, exist_ok=True)
    
    # Extract performance metrics
    mp_metrics = results.get('multiprocessing', {})
    dask_metrics = results.get('dask', {})
    
    if not mp_metrics or not dask_metrics:
        logger.warning("Insufficient data for visualizations")
        return
    
    # 1. Execution Time Comparison
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
    
    # Execution time bar chart
    methods = ['Multiprocessing', 'Dask']
    times_minutes = [
        mp_metrics.get('execution_time_minutes', 0),
        dask_metrics.get('execution_time_minutes', 0)
    ]
    
    bars = ax1.bar(methods, times_minutes, color=['#1f77b4', '#ff7f0e'])
    ax1.set_ylabel('Execution Time (minutes)')
    ax1.set_title('Total Execution Time Comparison')
    
    # Add value labels on bars
    for bar, time_val in zip(bars, times_minutes):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'{time_val:.2f}m', ha='center', va='bottom')
    
    # Memory usage comparison
    memory_metrics = ['Peak Memory (GB)', 'Avg Memory (GB)', 'Peak Process Memory (GB)']
    mp_memory = [
        mp_metrics.get('peak_memory_gb', 0),
        mp_metrics.get('avg_memory_gb', 0),
        mp_metrics.get('peak_process_memory_gb', 0)
    ]
    dask_memory = [
        dask_metrics.get('peak_memory_gb', 0),
        dask_metrics.get('avg_memory_gb', 0),
        dask_metrics.get('peak_process_memory_gb', 0)
    ]
    
    x = np.arange(len(memory_metrics))
    width = 0.35
    
    ax2.bar(x - width/2, mp_memory, width, label='Multiprocessing', color='#1f77b4')
    ax2.bar(x + width/2, dask_memory, width, label='Dask', color='#ff7f0e')
    ax2.set_ylabel('Memory Usage (GB)')
    ax2.set_title('Memory Usage Comparison')
    ax2.set_xticks(x)
    ax2.set_xticklabels(memory_metrics, rotation=45, ha='right')
    ax2.legend()
    
    # CPU usage comparison
    cpu_metrics = ['Peak CPU (%)', 'Avg CPU (%)']
    mp_cpu = [
        mp_metrics.get('peak_cpu_percent', 0),
        mp_metrics.get('avg_cpu_percent', 0)
    ]
    dask_cpu = [
        dask_metrics.get('peak_cpu_percent', 0),
        dask_metrics.get('avg_cpu_percent', 0)
    ]
    
    x = np.arange(len(cpu_metrics))
    ax3.bar(x - width/2, mp_cpu, width, label='Multiprocessing', color='#1f77b4')
    ax3.bar(x + width/2, dask_cpu, width, label='Dask', color='#ff7f0e')
    ax3.set_ylabel('CPU Usage (%)')
    ax3.set_title('CPU Usage Comparison')
    ax3.set_xticks(x)
    ax3.set_xticklabels(cpu_metrics)
    ax3.legend()
    
    # Performance efficiency (inverse of execution time)
    efficiency_mp = 1 / max(mp_metrics.get('execution_time_minutes', 1), 0.01)
    efficiency_dask = 1 / max(dask_metrics.get('execution_time_minutes', 1), 0.01)
    
    ax4.bar(methods, [efficiency_mp, efficiency_dask], color=['#1f77b4', '#ff7f0e'])
    ax4.set_ylabel('Performance Efficiency (1/minutes)')
    ax4.set_title('Performance Efficiency Comparison')
    
    plt.tight_layout()
    plt.savefig(viz_dir / 'performance_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. Time series plots of system resources (if monitoring data available)
    mp_monitoring = mp_metrics.get('monitoring_data', [])
    dask_monitoring = dask_metrics.get('monitoring_data', [])
    
    if mp_monitoring or dask_monitoring:
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        
        # Plot multiprocessing monitoring data
        if mp_monitoring:
            mp_df = pd.DataFrame(mp_monitoring)
            ax1.plot(mp_df['timestamp'], mp_df['cpu_percent'], label='Multiprocessing', color='#1f77b4')
            ax2.plot(mp_df['timestamp'], mp_df['memory_used_gb'], label='Multiprocessing', color='#1f77b4')
            ax3.plot(mp_df['timestamp'], mp_df['process_memory_gb'], label='Multiprocessing', color='#1f77b4')
            ax4.plot(mp_df['timestamp'], mp_df['process_cpu_percent'], label='Multiprocessing', color='#1f77b4')
        
        # Plot dask monitoring data
        if dask_monitoring:
            dask_df = pd.DataFrame(dask_monitoring)
            ax1.plot(dask_df['timestamp'], dask_df['cpu_percent'], label='Dask', color='#ff7f0e')
            ax2.plot(dask_df['timestamp'], dask_df['memory_used_gb'], label='Dask', color='#ff7f0e')
            ax3.plot(dask_df['timestamp'], dask_df['process_memory_gb'], label='Dask', color='#ff7f0e')
            ax4.plot(dask_df['timestamp'], dask_df['process_cpu_percent'], label='Dask', color='#ff7f0e')
        
        ax1.set_xlabel('Time (seconds)')
        ax1.set_ylabel('CPU Usage (%)')
        ax1.set_title('CPU Usage Over Time')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        ax2.set_xlabel('Time (seconds)')
        ax2.set_ylabel('System Memory (GB)')
        ax2.set_title('System Memory Usage Over Time')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        ax3.set_xlabel('Time (seconds)')
        ax3.set_ylabel('Process Memory (GB)')
        ax3.set_title('Process Memory Usage Over Time')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        ax4.set_xlabel('Time (seconds)')
        ax4.set_ylabel('Process CPU (%)')
        ax4.set_title('Process CPU Usage Over Time')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(viz_dir / 'resource_usage_timeline.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    logger.info(f"Visualizations saved to {viz_dir}")

def generate_performance_report(results: Dict, validation_results: Dict, output_dir: str):
    """Generate a comprehensive performance report."""
    logger.info("Generating performance report...")
    
    report_path = Path(output_dir) / 'performance_report.md'
    
    with open(report_path, 'w') as f:
        f.write("# Climate County Processing Performance Comparison\n\n")
        f.write(f"**Report Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # System Information
        f.write("## System Information\n\n")
        f.write(f"- **CPU Cores:** {psutil.cpu_count(logical=True)} logical, {psutil.cpu_count(logical=False)} physical\n")
        f.write(f"- **Total Memory:** {psutil.virtual_memory().total / 1024**3:.2f} GB\n")
        f.write(f"- **Python Version:** {sys.version}\n")
        f.write(f"- **Platform:** {os.uname().sysname} {os.uname().release}\n\n")
        
        # Test Configuration
        f.write("## Test Configuration\n\n")
        f.write("- **Climate Data:** `output/climate_means/conus_historical_climate_means_2012-2014.nc`\n")
        f.write("- **County Boundaries:** `output/county_boundaries/contiguous_us_counties.parquet`\n")
        f.write("- **Variable Processed:** `pr_2014` (precipitation sum)\n\n")
        
        # Performance Results
        f.write("## Performance Results\n\n")
        
        mp_metrics = results.get('multiprocessing', {})
        dask_metrics = results.get('dask', {})
        
        if mp_metrics and dask_metrics:
            f.write("| Metric | Multiprocessing | Dask | Improvement |\n")
            f.write("|--------|-----------------|------|-------------|\n")
            
            # Execution time
            mp_time = mp_metrics.get('execution_time_minutes', 0)
            dask_time = dask_metrics.get('execution_time_minutes', 0)
            if mp_time > 0 and dask_time > 0:
                speedup = mp_time / dask_time
                f.write(f"| Execution Time | {mp_time:.2f} min | {dask_time:.2f} min | {speedup:.2f}x |\n")
            
            # Memory usage
            mp_mem = mp_metrics.get('peak_memory_gb', 0)
            dask_mem = dask_metrics.get('peak_memory_gb', 0)
            if mp_mem > 0 and dask_mem > 0:
                mem_ratio = dask_mem / mp_mem
                f.write(f"| Peak Memory | {mp_mem:.2f} GB | {dask_mem:.2f} GB | {mem_ratio:.2f}x |\n")
            
            # CPU usage
            mp_cpu = mp_metrics.get('avg_cpu_percent', 0)
            dask_cpu = dask_metrics.get('avg_cpu_percent', 0)
            f.write(f"| Avg CPU Usage | {mp_cpu:.1f}% | {dask_cpu:.1f}% | - |\n")
            
            # Success status
            mp_success = "✅" if mp_metrics.get('success', False) else "❌"
            dask_success = "✅" if dask_metrics.get('success', False) else "❌"
            f.write(f"| Success | {mp_success} | {dask_success} | - |\n")
        
        f.write("\n")
        
        # Validation Results
        f.write("## Output Validation\n\n")
        if validation_results:
            f.write(f"- **Files Exist:** {'✅' if validation_results.get('files_exist', False) else '❌'}\n")
            f.write(f"- **Row Count Match:** {'✅' if validation_results.get('row_count_match', False) else '❌'}\n")
            f.write(f"- **Column Count Match:** {'✅' if validation_results.get('column_count_match', False) else '❌'}\n")
            f.write(f"- **Identical Results:** {'✅' if validation_results.get('identical_results', False) else '❌'}\n")
            
            details = validation_results.get('details', {})
            if 'multiprocessing_shape' in details:
                f.write(f"- **Multiprocessing Output Shape:** {details['multiprocessing_shape']}\n")
            if 'dask_shape' in details:
                f.write(f"- **Dask Output Shape:** {details['dask_shape']}\n")
        
        f.write("\n")
        
        # Detailed Analysis
        f.write("## Detailed Analysis\n\n")
        
        if mp_metrics.get('success', False) and dask_metrics.get('success', False):
            if dask_time < mp_time:
                speedup = mp_time / dask_time
                f.write(f"🚀 **Dask implementation is {speedup:.2f}x faster** than multiprocessing.\n\n")
            elif mp_time < dask_time:
                speedup = dask_time / mp_time
                f.write(f"🚀 **Multiprocessing implementation is {speedup:.2f}x faster** than Dask.\n\n")
            else:
                f.write("⚖️ **Both implementations have similar performance**.\n\n")
        
        # Recommendations
        f.write("## Recommendations\n\n")
        
        if mp_metrics.get('success', False) and dask_metrics.get('success', False):
            if dask_time < mp_time:
                f.write("- **Use Dask implementation** for better performance\n")
                f.write("- Dask provides better scalability and monitoring capabilities\n")
                f.write("- Consider using Dask for larger datasets or distributed processing\n")
            else:
                f.write("- **Use multiprocessing implementation** for simpler deployment\n")
                f.write("- Multiprocessing has fewer dependencies and is easier to debug\n")
                f.write("- Consider Dask for more complex workflows or larger scale processing\n")
        
        f.write("\n")
        f.write("## Files Generated\n\n")
        f.write("- `performance_comparison.png` - Performance metrics comparison\n")
        f.write("- `resource_usage_timeline.png` - Resource usage over time\n")
        f.write("- `performance_results.json` - Raw performance data\n")
        f.write("- `performance_comparison_YYYYMMDD_HHMMSS.log` - Detailed execution log\n")
    
    logger.info(f"Performance report saved to {report_path}")

def main():
    """Main function to run performance comparison."""
    logger.info("🚀 Starting Climate County Processing Performance Comparison")
    
    # Create output directory
    output_dir = Path("output/performance_comparison")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Check if required files exist
    climate_data_path = "output/climate_means/conus_historical_climate_means_2012-2014.nc"
    counties_path = "output/county_boundaries/contiguous_us_counties.parquet"
    
    if not os.path.exists(climate_data_path):
        logger.error(f"❌ Climate data file not found: {climate_data_path}")
        return
    
    if not os.path.exists(counties_path):
        logger.error(f"❌ County boundaries file not found: {counties_path}")
        return
    
    logger.info(f"✅ Using climate data: {climate_data_path}")
    logger.info(f"✅ Using county boundaries: {counties_path}")
    
    # Initialize results dictionary
    results = {}
    
    # Test 1: Multiprocessing Implementation
    logger.info("\n" + "="*60)
    logger.info("TESTING MULTIPROCESSING IMPLEMENTATION")
    logger.info("="*60)
    
    mp_test = PerformanceTest("multiprocessing")
    mp_results = mp_test.run_script("county_stats.py")
    results['multiprocessing'] = mp_results
    
    # Test 2: Dask Implementation
    logger.info("\n" + "="*60)
    logger.info("TESTING DASK IMPLEMENTATION")
    logger.info("="*60)
    
    dask_test = PerformanceTest("dask")
    dask_results = dask_test.run_script("county_stats_dask.py")
    results['dask'] = dask_results
    
    # Validate outputs
    logger.info("\n" + "="*60)
    logger.info("VALIDATING OUTPUTS")
    logger.info("="*60)
    
    mp_output = "output/county_climate_stats/county_climate_stats.csv"
    dask_output = "output/county_climate_stats/county_climate_stats_dask_simple.csv"
    
    validation_results = validate_output_files(mp_output, dask_output)
    
    # Save raw results
    results_path = output_dir / 'performance_results.json'
    with open(results_path, 'w') as f:
        # Remove monitoring data for JSON serialization (too large)
        results_for_json = {}
        for key, value in results.items():
            results_for_json[key] = {k: v for k, v in value.items() if k != 'monitoring_data'}
        
        json.dump({
            'results': results_for_json,
            'validation': validation_results,
            'timestamp': datetime.now().isoformat(),
            'system_info': {
                'cpu_count': psutil.cpu_count(logical=True),
                'memory_gb': psutil.virtual_memory().total / 1024**3,
                'python_version': sys.version
            }
        }, f, indent=2)
    
    logger.info(f"Raw results saved to {results_path}")
    
    # Generate visualizations
    create_performance_visualizations(results, str(output_dir))
    
    # Generate report
    generate_performance_report(results, validation_results, str(output_dir))
    
    # Print summary
    logger.info("\n" + "="*60)
    logger.info("PERFORMANCE COMPARISON SUMMARY")
    logger.info("="*60)
    
    mp_success = results['multiprocessing'].get('success', False)
    dask_success = results['dask'].get('success', False)
    
    if mp_success and dask_success:
        mp_time = results['multiprocessing'].get('execution_time_minutes', 0)
        dask_time = results['dask'].get('execution_time_minutes', 0)
        
        logger.info(f"Multiprocessing: {mp_time:.2f} minutes")
        logger.info(f"Dask: {dask_time:.2f} minutes")
        
        if dask_time < mp_time:
            speedup = mp_time / dask_time
            logger.info(f"🚀 Dask is {speedup:.2f}x faster!")
        elif mp_time < dask_time:
            speedup = dask_time / mp_time
            logger.info(f"🚀 Multiprocessing is {speedup:.2f}x faster!")
        else:
            logger.info("⚖️ Both implementations have similar performance")
        
        if validation_results.get('identical_results', False):
            logger.info("✅ Both implementations produce identical results")
        else:
            logger.info("⚠️ Output validation found differences")
    
    else:
        if not mp_success:
            logger.error("❌ Multiprocessing implementation failed")
        if not dask_success:
            logger.error("❌ Dask implementation failed")
    
    logger.info(f"\n📊 Detailed results available in: {output_dir}")
    logger.info("🎉 Performance comparison completed!")

if __name__ == '__main__':
    main() 