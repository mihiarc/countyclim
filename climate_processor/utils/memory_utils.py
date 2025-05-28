# utils/memory_utils.py
"""
Memory Monitoring Utilities

Provides memory monitoring and management utilities for the climate processor.
"""

import logging
import psutil
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)


def log_memory_usage(stage: str = "") -> None:
    """
    Log current memory usage.
    
    Args:
        stage: Description of current processing stage
    """
    try:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024**2
        memory_gb = memory_mb / 1024
        
        # Get system memory info
        system_memory = psutil.virtual_memory()
        available_gb = system_memory.available / 1024**3
        percent_used = (memory_mb / (system_memory.total / 1024**2)) * 100
        
        stage_text = f" {stage}" if stage else ""
        logger.info(f"Memory usage{stage_text}: {memory_mb:.1f} MB ({memory_gb:.2f} GB) "
                   f"- {percent_used:.1f}% of system, {available_gb:.1f} GB available")
        
    except Exception as e:
        logger.warning(f"Could not get memory usage: {e}")


def check_memory_threshold(threshold_percent: float = 80.0) -> bool:
    """
    Check if memory usage is below threshold.
    
    Args:
        threshold_percent: Memory usage threshold percentage
        
    Returns:
        True if below threshold, False otherwise
    """
    try:
        memory = psutil.virtual_memory()
        if memory.percent > threshold_percent:
            logger.warning(f"High memory usage: {memory.percent:.1f}% (threshold: {threshold_percent}%)")
            return False
        return True
    except Exception as e:
        logger.warning(f"Could not check memory threshold: {e}")
        return True
    
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