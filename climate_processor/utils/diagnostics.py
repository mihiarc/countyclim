# utils/diagnostics.py
"""
System Diagnostics Module

Provides comprehensive system diagnostics and validation utilities.
"""

import logging
import platform
import multiprocessing as mp
from pathlib import Path
from typing import Dict, Any, List

import psutil

from ..climate_procesor.file_handler import create_file_handler
from ..climate_procesor.config import get_config_summary

logger = logging.getLogger(__name__)


def get_memory_recommendations(config: Dict) -> List[str]:
    """
    Get memory optimization recommendations based on current system.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        List of recommendation strings
    """
    recommendations = []
    
    try:
        memory = psutil.virtual_memory()
        total_gb = memory.total / 1024**3
        
        if total_gb < 8:
            recommendations.append("Consider upgrading to at least 8GB RAM for optimal performance")
            recommendations.append("Reduce batch_size to 10 or lower")
            recommendations.append("Reduce max_workers to 2")
        elif total_gb < 16:
            recommendations.append("Good memory for climate processing")
            recommendations.append("Consider batch_size of 15-20")
        else:
            recommendations.append("Excellent memory for climate processing")
            recommendations.append("Can use larger batch_size (25-50)")
        
        # Check current configuration
        batch_size = config.get('batch_size', 20)
        max_workers = config.get('max_workers', 4)
        
        memory_per_worker = total_gb / max_workers
        if memory_per_worker < 2:
            recommendations.append(f"Reduce max_workers (current: {max_workers}) - insufficient memory per worker")
        
        estimated_memory_usage = batch_size * max_workers * 0.5  # Rough estimate
        if estimated_memory_usage > total_gb * 0.7:
            recommendations.append(f"Reduce batch_size (current: {batch_size}) - may exceed memory limits")
        
    except Exception as e:
        logger.warning(f"Could not generate memory recommendations: {e}")
        recommendations.append("Could not analyze memory - monitor usage carefully")
    
    return recommendations


def get_system_info() -> Dict[str, Any]:
    """
    Get comprehensive system information.
    
    Returns:
        Dictionary with system information
    """
    try:
        system_info = {
            'platform': platform.system(),
            'platform_release': platform.release(),
            'platform_version': platform.version(),
            'python_version': platform.python_version(),
            'cpu_count': mp.cpu_count(),
            'cpu_freq': psutil.cpu_freq()._asdict() if psutil.cpu_freq() else None,
            'memory': {
                'total_gb': psutil.virtual_memory().total / 1024**3,
                'available_gb': psutil.virtual_memory().available / 1024**3,
                'percent_used': psutil.virtual_memory().percent
            },
            'disk': {}
        }
        
        # Disk information for common mount points
        disk_paths = ['.', '/tmp', '/']
        for path in disk_paths:
            if Path(path).exists():
                try:
                    disk = psutil.disk_usage(path)
                    system_info['disk'][path] = {
                        'total_gb': disk.total / 1024**3,
                        'free_gb': disk.free / 1024**3,
                        'percent_used': ((disk.total - disk.free) / disk.total) * 100
                    }
                except:
                    pass
        
        return system_info
        
    except Exception as e:
        logger.error(f"Could not get system info: {e}")
        return {}


def validate_file_handler(config: Dict[str, Any]) -> bool:
    """
    Validate file handler and data availability.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if validation successful, False otherwise
    """
    try:
        logger.info("=== File Handler Validation ===")
        
        file_handler = create_file_handler(
            external_drive_path=config['external_drive_path'],
            variables=config['variables'],
            scenarios=[config['active_scenario']]
        )
        
        if not file_handler.validate_base_path():
            logger.error("✗ Base path validation failed")
            return False
        
        logger.info("✓ Base path validation passed")
        
        # Quick file discovery
        available_files = file_handler.discover_available_files()
        
        has_data = False
        for scenario, vars_dict in available_files.items():
            for var, file_list in vars_dict.items():
                if file_list:
                    years = [year for year, _ in file_list]
                    logger.info(f"✓ {scenario}/{var}: {len(file_list)} files ({min(years)}-{max(years)})")
                    has_data = True
                else:
                    logger.warning(f"✗ {scenario}/{var}: No files found")
        
        return has_data
        
    except Exception as e:
        logger.error(f"File handler validation failed: {e}")
        return False


def check_dependencies() -> Dict[str, bool]:
    """
    Check that all required dependencies are available.
    
    Returns:
        Dictionary mapping dependency names to availability
    """
    dependencies = {}
    
    required_packages = [
        'numpy', 'xarray', 'dask', 'netcdf4', 'psutil', 
        'tqdm', 'pandas', 'distributed'
    ]
    
    for package in required_packages:
        try:
            __import__(package)
            dependencies[package] = True
            logger.debug(f"✓ {package} available")
        except ImportError:
            dependencies[package] = False
            logger.error(f"✗ {package} not available")
    
    return dependencies


def run_system_diagnostics(config: Dict[str, Any]) -> None:
    """
    Run comprehensive system diagnostics.
    
    Args:
        config: Configuration dictionary
    """
    logger.info("=== Climate Data Processor Diagnostics ===")
    
    # System information
    logger.info("=== System Information ===")
    system_info = get_system_info()
    
    if system_info:
        logger.info(f"Platform: {system_info['platform']} {system_info['platform_release']}")
        logger.info(f"Python version: {system_info['python_version']}")
        logger.info(f"CPU cores: {system_info['cpu_count']}")
        
        memory = system_info['memory']
        logger.info(f"Memory: {memory['total_gb']:.1f} GB total, "
                   f"{memory['available_gb']:.1f} GB available ({memory['percent_used']:.1f}% used)")
        
        for path, disk_info in system_info['disk'].items():
            logger.info(f"Disk {path}: {disk_info['free_gb']:.1f} GB free of {disk_info['total_gb']:.1f} GB")
    
    # Configuration summary
    logger.info(get_config_summary(config))
    
    # Dependency check
    logger.info("=== Dependency Check ===")
    dependencies = check_dependencies()
    all_deps_ok = all(dependencies.values())
    
    if all_deps_ok:
        logger.info("✓ All required dependencies available")
    else:
        missing = [pkg for pkg, available in dependencies.items() if not available]
        logger.error(f"✗ Missing dependencies: {missing}")
        logger.error("Please install missing packages: pip install -r requirements.txt")
    
    # File handler validation
    if all_deps_ok:
        validate_file_handler(config)
    
    # Memory recommendations
    logger.info("=== Memory Recommendations ===")
    recommendations = get_memory_recommendations(config)
    for rec in recommendations:
        logger.info(f"💡 {rec}")
    
    # Performance estimates
    logger.info("=== Performance Estimates ===")
    if system_info:
        memory_gb = system_info['memory']['total_gb']
        cpu_count = system_info['cpu_count']
        
        if memory_gb >= 16 and cpu_count >= 8:
            logger.info("🚀 Excellent performance expected")
        elif memory_gb >= 8 and cpu_count >= 4:
            logger.info("✅ Good performance expected")
        else:
            logger.info("⚠️  Limited performance expected - consider reducing batch size")
    
    logger.info("=== Diagnostics Complete ===")