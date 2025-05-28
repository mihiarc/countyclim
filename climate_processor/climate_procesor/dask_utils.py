"""
Dask Utilities Module

Handles Dask cluster setup, resource optimization, and distributed computing
configuration for the climate data processing pipeline.
"""

import logging
import multiprocessing as mp
from typing import Dict, Any, Tuple
import psutil
from dask.distributed import Client, LocalCluster
from dask.diagnostics import ProgressBar

logger = logging.getLogger(__name__)


def configure_dask_resources(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Configure Dask resources based on system capabilities and configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Dictionary with Dask configuration parameters
    """
    total_memory = psutil.virtual_memory().total
    available_memory = psutil.virtual_memory().available
    
    logger.info(f"System memory - Total: {total_memory / 1024**3:.1f}GB, "
               f"Available: {available_memory / 1024**3:.1f}GB")
    
    # Be conservative with memory allocation
    # Use at most 50% of available memory, min 2GB per worker, max 4GB per worker
    memory_per_worker = min(
        4 * 1024**3,  # 4GB max per worker
        max(2 * 1024**3, available_memory * 0.5 // 4)  # At least 2GB
    )
    
    # Calculate number of workers based on memory constraints
    max_workers_by_memory = int(available_memory * 0.5 // memory_per_worker)
    max_workers_by_cpu = max(1, mp.cpu_count() - 1)  # Leave one core free
    
    # Use configured max_workers as upper limit
    n_workers = min(max_workers_by_memory, max_workers_by_cpu, config['max_workers'])
    
    # Allow some threading for I/O operations
    threads_per_worker = min(2, max(1, mp.cpu_count() // n_workers))
    
    dask_config = {
        'n_workers': n_workers,
        'memory_limit': f'{int(memory_per_worker / 1024**3)}GB',
        'threads_per_worker': threads_per_worker
    }
    
    logger.info(f"Dask configuration: {dask_config}")
    return dask_config


def setup_dask_client(config: Dict[str, Any]) -> Tuple[Client, LocalCluster]:
    """
    Set up and return a Dask distributed client with optimized configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Tuple of (client, cluster)
    """
    logger.info("Initializing optimized Dask distributed computing...")
    
    dask_config = configure_dask_resources(config)
    
    cluster = LocalCluster(
        **dask_config,
        dashboard_address=':8787',
        silence_logs=logging.ERROR,  # Reduce log noise
        death_timeout='60s',  # Handle worker failures gracefully
        processes=True,  # Use processes for better isolation
    )
    
    client = Client(cluster)
    
    # Enable progress reporting
    ProgressBar().register()
    
    logger.info(f"Dask dashboard available at: {client.dashboard_link}")
    logger.info(f"Cluster info: {cluster}")
    logger.info(f"Workers: {len(client.scheduler_info()['workers'])}")
    
    return client, cluster


def get_optimal_chunks(file_path: str, config: Dict[str, Any]) -> Dict[str, int]:
    """
    Determine optimal chunk size based on data dimensions and available memory.
    
    Args:
        file_path: Path to NetCDF file
        config: Configuration dictionary
        
    Returns:
        Dictionary with optimal chunk sizes
    """
    import xarray as xr
    from pathlib import Path
    
    try:
        with xr.open_dataset(file_path, decode_times=False) as ds:
            time_size = ds.dims.get('time', 365)
            lat_size = ds.dims.get('lat', 100)
            lon_size = ds.dims.get('lon', 100)
            
            # Use configured target chunk size
            target_chunk_size = config.get('target_chunk_size', 75) * 1024 * 1024  # Convert MB to bytes
            element_size = 8  # 8 bytes for float64
            
            # Calculate optimal time chunking (at least 1 year, max 5 years)
            time_chunk = min(time_size, max(365, min(365 * 5, time_size // 5)))
            
            # Calculate if we need spatial chunking
            spatial_elements = lat_size * lon_size
            total_elements = spatial_elements * time_chunk
            
            if total_elements * element_size > target_chunk_size:
                # Need spatial chunking
                spatial_chunk = int((target_chunk_size // (time_chunk * element_size)) ** 0.5)
                spatial_chunk = max(10, min(spatial_chunk, 50))  # Reasonable bounds
                
                chunks = {
                    'time': time_chunk,
                    'lat': min(lat_size, spatial_chunk),
                    'lon': min(lon_size, spatial_chunk)
                }
            else:
                chunks = {'time': time_chunk}
            
            logger.debug(f"Optimal chunks for {Path(file_path).name}: {chunks}")
            return chunks
            
    except Exception as e:
        logger.warning(f"Could not determine optimal chunks for {file_path}: {e}")
        # Fallback to conservative chunking
        return {'time': 365, 'lat': 25, 'lon': 25}


def open_dataset_optimized(file_path: str, chunks: Dict[str, int]) -> 'xr.Dataset':
    """
    Open dataset with optimization for climate data.
    
    Args:
        file_path: Path to NetCDF file
        chunks: Chunk sizes for dask arrays
        
    Returns:
        Opened xarray Dataset
    """
    import xarray as xr
    
    return xr.open_dataset(
        file_path,
        chunks=chunks,
        engine='netcdf4',  # Usually faster than h5netcdf for large files
        decode_times=True,
        use_cftime=True,   # Handle climate calendars properly
        parallel=True,     # Enable parallel reading where possible
        cache=False        # Don't cache to save memory
    )


def get_dask_performance_report(client: Client) -> Dict[str, Any]:
    """
    Get performance statistics from Dask client.
    
    Args:
        client: Dask client instance
        
    Returns:
        Dictionary with performance metrics
    """
    try:
        scheduler_info = client.scheduler_info()
        
        # Worker statistics
        workers = scheduler_info.get('workers', {})
        n_workers = len(workers)
        
        # Memory statistics
        total_memory = sum(w.get('memory_limit', 0) for w in workers.values())
        used_memory = sum(w.get('metrics', {}).get('memory', 0) for w in workers.values())
        
        # Task statistics
        processing = scheduler_info.get('processing', {})
        n_tasks_processing = len(processing)
        
        return {
            'n_workers': n_workers,
            'total_memory_gb': total_memory / 1024**3,
            'used_memory_gb': used_memory / 1024**3,
            'memory_utilization': (used_memory / total_memory * 100) if total_memory > 0 else 0,
            'tasks_processing': n_tasks_processing,
            'dashboard_link': getattr(client, 'dashboard_link', 'N/A')
        }
    except Exception as e:
        logger.warning(f"Could not get Dask performance report: {e}")
        return {}


def cleanup_dask_resources(client: Client, cluster: LocalCluster) -> None:
    """
    Clean up Dask resources properly.
    
    Args:
        client: Dask client to close
        cluster: Dask cluster to close
    """
    logger.info("Cleaning up Dask resources...")
    
    try:
        # Get final performance report
        perf_report = get_dask_performance_report(client)
        if perf_report:
            logger.info(f"Final Dask statistics:")
            logger.info(f"  Workers: {perf_report['n_workers']}")
            logger.info(f"  Memory utilization: {perf_report['memory_utilization']:.1f}%")
            logger.info(f"  Tasks processing: {perf_report['tasks_processing']}")
        
        # Clear any remaining computations
        client.restart()
        client.close()
        cluster.close()
        
        logger.info("✓ Dask resources cleaned up successfully")
        
    except Exception as e:
        logger.warning(f"Error during Dask cleanup: {e}")


def monitor_dask_memory(client: Client, threshold: float = 80.0) -> bool:
    """
    Monitor Dask memory usage and warn if approaching limits.
    
    Args:
        client: Dask client instance
        threshold: Memory usage percentage threshold for warnings
        
    Returns:
        True if memory usage is below threshold, False otherwise
    """
    try:
        perf_report = get_dask_performance_report(client)
        memory_util = perf_report.get('memory_utilization', 0)
        
        if memory_util > threshold:
            logger.warning(f"High Dask memory usage: {memory_util:.1f}%")
            logger.warning("Consider reducing batch size or number of workers")
            return False
        else:
            logger.debug(f"Dask memory usage: {memory_util:.1f}%")
            return True
            
    except Exception as e:
        logger.warning(f"Could not monitor Dask memory: {e}")
        return True