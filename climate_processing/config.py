"""
Configuration module for climate data processing.

This module centralizes all configuration settings and supports environment variables
for flexibility across different deployment environments.
"""

import os
import multiprocessing as mp
import psutil
from pathlib import Path
from typing import Dict, Any, Optional


class ClimateConfig:
    """Configuration class for climate data processing parameters."""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize configuration with defaults and optional overrides.
        
        Args:
            config_dict: Optional dictionary to override default settings
        """
        # Load from environment variables with defaults
        self.external_drive_path = os.getenv('CLIMATE_EXTERNAL_DRIVE', '/Volumes/RPA1TB')
        self.base_data_path = os.getenv('CLIMATE_BASE_DATA_PATH', 
                                        f'{self.external_drive_path}/data/NorESM2-LM')
        self.output_dir = os.getenv('CLIMATE_OUTPUT_DIR', 'output/climate_means')
        
        # Processing configuration
        self.parallel_processing = os.getenv('CLIMATE_PARALLEL_PROCESSING', 'True').lower() == 'true'
        self.max_processes = int(os.getenv('CLIMATE_MAX_PROCESSES', str(mp.cpu_count() - 2)))
        
        # Dask configuration
        self.chunk_size = {'time': int(os.getenv('CLIMATE_CHUNK_SIZE', '365'))}
        self.memory_limit = os.getenv('CLIMATE_MEMORY_LIMIT', 
                                     f'{int(psutil.virtual_memory().total / (1024**3))}GB')
        
        # Active scenario
        self.active_scenario = os.getenv('CLIMATE_ACTIVE_SCENARIO', 'historical')
        
        # Apply any overrides from config_dict
        if config_dict:
            for key, value in config_dict.items():
                setattr(self, key, value)
        
        # Create output directory if it doesn't exist
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
    
    @property
    def scenarios(self) -> Dict[str, Dict[str, str]]:
        """Get scenario file patterns based on base data path."""
        return {
            'historical': {
                'tas': f'{self.base_data_path}/tas/historical/tas_day_*.nc',
                'tasmax': f'{self.base_data_path}/tasmax/historical/tasmax_day_*.nc',
                'tasmin': f'{self.base_data_path}/tasmin/historical/tasmin_day_*.nc',
                'pr': f'{self.base_data_path}/pr/historical/pr_day_*.nc'
            },
            'ssp126': { 
                'tas': f'{self.base_data_path}/tas/ssp126/tas_day_*.nc',
                'tasmax': f'{self.base_data_path}/tasmax/ssp126/tasmax_day_*.nc',
                'tasmin': f'{self.base_data_path}/tasmin/ssp126/tasmin_day_*.nc',
                'pr': f'{self.base_data_path}/pr/ssp126/pr_day_*.nc'
            },
            'ssp245': { 
                'tas': f'{self.base_data_path}/tas/ssp245/tas_day_*.nc',
                'tasmax': f'{self.base_data_path}/tasmax/ssp245/tasmax_day_*.nc',
                'tasmin': f'{self.base_data_path}/tasmin/ssp245/tasmin_day_*.nc',
                'pr': f'{self.base_data_path}/pr/ssp245/pr_day_*.nc'
            },
            'ssp370': { 
                'tas': f'{self.base_data_path}/tas/ssp370/tas_day_*.nc',
                'tasmax': f'{self.base_data_path}/tasmax/ssp370/tasmax_day_*.nc',
                'tasmin': f'{self.base_data_path}/tasmin/ssp370/tasmin_day_*.nc',
                'pr': f'{self.base_data_path}/pr/ssp370/pr_day_*.nc'
            },
            'ssp585': { 
                'tas': f'{self.base_data_path}/tas/ssp585/tas_day_*.nc',
                'tasmax': f'{self.base_data_path}/tasmax/ssp585/tasmax_day_*.nc',
                'tasmin': f'{self.base_data_path}/tasmin/ssp585/tasmin_day_*.nc',
                'pr': f'{self.base_data_path}/pr/ssp585/pr_day_*.nc'
            }
        }
    
    @property
    def data_availability(self) -> Dict[str, Dict[str, int]]:
        """Get data availability periods for each scenario."""
        return {
            'historical': {'start': 1950, 'end': 2014},
            'ssp126': {'start': 2015, 'end': 2100},
            'ssp245': {'start': 2015, 'end': 2100},
            'ssp370': {'start': 2015, 'end': 2100},
            'ssp585': {'start': 2015, 'end': 2100}
        }
    
    def validate(self) -> bool:
        """Validate configuration settings."""
        # Check if external drive path exists
        if not Path(self.external_drive_path).exists():
            raise ValueError(f"External drive path does not exist: {self.external_drive_path}")
        
        # Check if base data path exists
        if not Path(self.base_data_path).exists():
            raise ValueError(f"Base data path does not exist: {self.base_data_path}")
        
        # Check if active scenario is valid
        if self.active_scenario not in self.scenarios:
            raise ValueError(f"Invalid scenario: {self.active_scenario}. "
                           f"Must be one of {list(self.scenarios.keys())}")
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'external_drive_path': self.external_drive_path,
            'base_data_path': self.base_data_path,
            'output_dir': self.output_dir,
            'parallel_processing': self.parallel_processing,
            'max_processes': self.max_processes,
            'chunk_size': self.chunk_size,
            'memory_limit': self.memory_limit,
            'active_scenario': self.active_scenario,
            'scenarios': self.scenarios,
            'data_availability': self.data_availability
        }
    
    def __repr__(self) -> str:
        """String representation of configuration."""
        return f"ClimateConfig(scenario='{self.active_scenario}', output='{self.output_dir}')" 