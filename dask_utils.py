#!/usr/bin/env python3
"""
Dask Configuration and Monitoring Utilities

This script provides utilities for configuring and monitoring Dask performance
for climate data processing workloads.
"""

import os
import psutil
import time
from pathlib import Path
from typing import Dict, Any, Optional
import yaml

from climate_processing import ClimateConfig


def analyze_system_resources() -> Dict[str, Any]:
    """Analyze system resources and provide Dask configuration recommendations."""
    
    # Get system information
    cpu_count = psutil.cpu_count()
    memory_gb = psutil.virtual_memory().total / (1024**3)
    disk_usage = psutil.disk_usage('.')
    
    # Calculate recommendations
    recommended_workers = max(2, cpu_count // 2)  # Conservative approach
    recommended_memory_per_worker = (memory_gb * 0.8) / recommended_workers
    
    analysis = {
        'system': {
            'cpu_cores': cpu_count,
            'total_memory_gb': round(memory_gb, 1),
            'available_memory_gb': round(psutil.virtual_memory().available / (1024**3), 1),
            'disk_free_gb': round(disk_usage.free / (1024**3), 1),
        },
        'recommendations': {
            'max_processes': recommended_workers,
            'memory_limit_per_worker': f'{recommended_memory_per_worker:.1f}GB',
            'threads_per_worker': 2,  # Good for I/O bound operations
            'total_cluster_memory': f'{recommended_memory_per_worker * recommended_workers:.1f}GB'
        },
        'warnings': []
    }
    
    # Add warnings based on system state
    if psutil.virtual_memory().percent > 80:
        analysis['warnings'].append("High memory usage detected. Consider reducing max_processes.")
    
    if disk_usage.free < 10 * (1024**3):  # Less than 10GB free
        analysis['warnings'].append("Low disk space. Dask may need disk space for spilling.")
    
    if cpu_count < 4:
        analysis['warnings'].append("Limited CPU cores. Consider reducing parallelism.")
    
    return analysis


def generate_optimal_config(output_file: str = 'config_optimized.yaml') -> None:
    """Generate an optimized configuration file based on system analysis."""
    
    analysis = analyze_system_resources()
    
    # Load current config as template
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print("Warning: config.yaml not found, creating from scratch")
        config = {}
    
    # Update with optimized settings
    config.update({
        'parallel_processing': True,
        'max_processes': analysis['recommendations']['max_processes'],
        'memory_limit': analysis['recommendations']['memory_limit_per_worker'],
        'dask_config': {
            'chunk_size': {
                'time': 365,
                'spatial': 'auto'
            },
            'memory_target': 0.8,
            'memory_spill': 0.9,
            'memory_pause': 0.95,
            'memory_terminate': 0.98,
            'threads_per_worker': analysis['recommendations']['threads_per_worker'],
            'processes': True,
            'work_stealing': True,
            'connect_timeout': '60s',
            'tcp_timeout': '60s',
            'allowed_failures': 3,
            'dashboard_port': 8787,
            'silence_logs': False
        }
    })
    
    # Save optimized config
    with open(output_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, indent=2)
    
    print(f"Optimized configuration saved to {output_file}")
    print("\nSystem Analysis:")
    print(f"  CPU cores: {analysis['system']['cpu_cores']}")
    print(f"  Total memory: {analysis['system']['total_memory_gb']} GB")
    print(f"  Available memory: {analysis['system']['available_memory_gb']} GB")
    print(f"  Free disk space: {analysis['system']['disk_free_gb']} GB")
    
    print("\nRecommended Settings:")
    print(f"  Workers: {analysis['recommendations']['max_processes']}")
    print(f"  Memory per worker: {analysis['recommendations']['memory_limit_per_worker']}")
    print(f"  Total cluster memory: {analysis['recommendations']['total_cluster_memory']}")
    
    if analysis['warnings']:
        print("\nWarnings:")
        for warning in analysis['warnings']:
            print(f"  ⚠️  {warning}")


def monitor_dask_performance(duration_minutes: int = 5) -> None:
    """Monitor system performance during Dask operations."""
    
    print(f"Monitoring system performance for {duration_minutes} minutes...")
    print("Press Ctrl+C to stop monitoring early")
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    
    try:
        while time.time() < end_time:
            # Get current system stats
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk_io = psutil.disk_io_counters()
            
            # Print stats
            elapsed = int(time.time() - start_time)
            print(f"[{elapsed:03d}s] CPU: {cpu_percent:5.1f}% | "
                  f"Memory: {memory.percent:5.1f}% ({memory.used/(1024**3):5.1f}GB used) | "
                  f"Disk I/O: {disk_io.read_bytes/(1024**2):6.1f}MB read, "
                  f"{disk_io.write_bytes/(1024**2):6.1f}MB write")
            
            time.sleep(5)  # Update every 5 seconds
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")


def test_dask_setup() -> None:
    """Test the current Dask configuration with a simple workload."""
    
    print("Testing Dask setup...")
    
    try:
        from climate_processing import ClimateConfig
        from climate_processing.processors import ClimateProcessor
        
        # Load configuration
        config = ClimateConfig()
        processor = ClimateProcessor(config)
        
        # Test Dask setup
        processor.setup_dask()
        
        print("\n✅ Dask cluster started successfully!")
        print("You can now access the dashboard to monitor performance.")
        print(f"Dashboard URL: {processor.client.dashboard_link}")
        
        # Simple test computation
        import dask.array as da
        import numpy as np
        
        print("\nRunning simple test computation...")
        x = da.random.random((10000, 10000), chunks=(1000, 1000))
        result = x.mean().compute()
        print(f"Test computation result: {result:.6f}")
        
        # Cleanup
        processor.cleanup_dask()
        print("\n✅ Dask test completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Dask test failed: {e}")
        print("Check your configuration and try again.")


def create_dask_workspace() -> None:
    """Create and configure Dask workspace directory."""
    
    workspace_dir = Path('./dask-worker-space')
    workspace_dir.mkdir(exist_ok=True)
    
    # Create subdirectories
    (workspace_dir / 'spill').mkdir(exist_ok=True)
    (workspace_dir / 'logs').mkdir(exist_ok=True)
    
    print(f"Dask workspace created at {workspace_dir.absolute()}")
    
    # Create .gitignore for workspace
    gitignore_content = """# Dask worker space - temporary files
*
!.gitignore
"""
    
    with open(workspace_dir / '.gitignore', 'w') as f:
        f.write(gitignore_content)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Dask Configuration and Monitoring Utilities')
    parser.add_argument('command', choices=['analyze', 'optimize', 'monitor', 'test', 'workspace'],
                       help='Command to run')
    parser.add_argument('--duration', type=int, default=5,
                       help='Duration for monitoring in minutes (default: 5)')
    parser.add_argument('--output', type=str, default='config_optimized.yaml',
                       help='Output file for optimized config (default: config_optimized.yaml)')
    
    args = parser.parse_args()
    
    if args.command == 'analyze':
        analysis = analyze_system_resources()
        print("System Analysis:")
        print(yaml.dump(analysis, default_flow_style=False, indent=2))
    
    elif args.command == 'optimize':
        generate_optimal_config(args.output)
    
    elif args.command == 'monitor':
        monitor_dask_performance(args.duration)
    
    elif args.command == 'test':
        test_dask_setup()
    
    elif args.command == 'workspace':
        create_dask_workspace() 