"""
Configuration module for climate data processing.

This module centralizes all configuration settings and supports both YAML configuration files
and environment variables for flexibility across different deployment environments.
"""

import os
import yaml
import multiprocessing as mp
import psutil
from pathlib import Path
from typing import Dict, Any, Optional


class ClimateConfig:
    """Configuration class for climate data processing parameters."""
    
    def __init__(self, config_file: Optional[str] = None, config_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize configuration with defaults, YAML file, and optional overrides.
        
        Args:
            config_file: Path to YAML configuration file (defaults to 'config.yaml')
            config_dict: Optional dictionary to override settings
        """
        # Get project root directory (assuming config.py is in climate_processing/)
        self.project_root = Path(__file__).parent.parent
        
        # Load configuration from YAML file first
        yaml_config = self._load_yaml_config(config_file)
        
        # Set defaults with YAML overrides, then environment variable overrides
        self._set_configuration(yaml_config)
        
        # Apply any overrides from config_dict
        if config_dict:
            for key, value in config_dict.items():
                setattr(self, key, value)
        
        # Create output directory if it doesn't exist
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
    
    def _load_yaml_config(self, config_file: Optional[str] = None) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if config_file is None:
            config_file = self.project_root / 'config.yaml'
        else:
            config_file = Path(config_file)
        
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    yaml_config = yaml.safe_load(f) or {}
                print(f"Loaded configuration from {config_file}")
                return yaml_config
            except Exception as e:
                print(f"Warning: Could not load YAML config from {config_file}: {e}")
                return {}
        else:
            print(f"No YAML config file found at {config_file}, using defaults")
            return {}
    
    def _set_configuration(self, yaml_config: Dict[str, Any]):
        """Set configuration values from YAML config and environment variables."""
        # Handle path expansion for external_drive_path and base_data_path
        yaml_external_drive = yaml_config.get('external_drive_path', str(self.project_root / 'data'))
        yaml_base_data = yaml_config.get('base_data_path')
        
        # Expand variables in YAML config (e.g., ${external_drive_path})
        if yaml_base_data and '${external_drive_path}' in yaml_base_data:
            yaml_base_data = yaml_base_data.replace('${external_drive_path}', yaml_external_drive)
        elif yaml_base_data is None:
            yaml_base_data = str(self.project_root / 'data' / 'NorESM2-LM')
        
        # Load from environment variables with YAML defaults, then project defaults
        self.external_drive_path = os.getenv('CLIMATE_EXTERNAL_DRIVE', yaml_external_drive)
        self.base_data_path = os.getenv('CLIMATE_BASE_DATA_PATH', yaml_base_data)
        self.output_dir = os.getenv('CLIMATE_OUTPUT_DIR', 
                                   yaml_config.get('output_dir', 'output/climate_means'))
        
        # Processing configuration
        self.parallel_processing = os.getenv('CLIMATE_PARALLEL_PROCESSING', 
                                           str(yaml_config.get('parallel_processing', True))).lower() == 'true'
        self.max_processes = int(os.getenv('CLIMATE_MAX_PROCESSES', 
                                         str(yaml_config.get('max_processes', mp.cpu_count() - 2))))
        
        # Dask configuration
        yaml_chunk_size = yaml_config.get('chunk_size', {})
        self.chunk_size = {'time': int(os.getenv('CLIMATE_CHUNK_SIZE', 
                                               str(yaml_chunk_size.get('time', 365))))}
        self.memory_limit = os.getenv('CLIMATE_MEMORY_LIMIT', 
                                     yaml_config.get('memory_limit', 
                                                   f'{int(psutil.virtual_memory().total / (1024**3))}GB'))
        
        # Active scenario
        self.active_scenario = os.getenv('CLIMATE_ACTIVE_SCENARIO', 
                                       yaml_config.get('active_scenario', 'historical'))
        
        # Data availability (use YAML if provided, otherwise use defaults)
        self._data_availability = yaml_config.get('data_availability', self._default_data_availability())
    
    def _default_data_availability(self) -> Dict[str, Dict[str, int]]:
        """Get default data availability periods."""
        return {
            'historical': {'start': 1950, 'end': 2014},
            'ssp126': {'start': 2015, 'end': 2100},
            'ssp245': {'start': 2015, 'end': 2100},
            'ssp370': {'start': 2015, 'end': 2100},
            'ssp585': {'start': 2015, 'end': 2100}
        }
    
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
        return self._data_availability
    
    def validate(self) -> bool:
        """Validate configuration settings."""
        # Check if external drive path exists (now local data directory)
        if not Path(self.external_drive_path).exists():
            print(f"Warning: Data directory does not exist: {self.external_drive_path}")
            print("You may need to create the data directory and add climate data files.")
        
        # Check if base data path exists
        if not Path(self.base_data_path).exists():
            print(f"Warning: Base data path does not exist: {self.base_data_path}")
            print("You may need to create the data directory structure and add climate data files.")
        
        # Check if active scenario is valid
        if self.active_scenario not in self.scenarios:
            raise ValueError(f"Invalid scenario: {self.active_scenario}. "
                           f"Must be one of {list(self.scenarios.keys())}")
        
        return True
    
    def save_config(self, output_file: Optional[str] = None):
        """Save current configuration to YAML file."""
        if output_file is None:
            output_file = self.project_root / 'config.yaml'
        
        config_dict = {
            'external_drive_path': self.external_drive_path,
            'base_data_path': self.base_data_path,
            'output_dir': self.output_dir,
            'active_scenario': self.active_scenario,
            'parallel_processing': self.parallel_processing,
            'max_processes': self.max_processes,
            'memory_limit': self.memory_limit,
            'chunk_size': self.chunk_size,
            'data_availability': self.data_availability
        }
        
        with open(output_file, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False, indent=2)
        
        print(f"Configuration saved to {output_file}")
    
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