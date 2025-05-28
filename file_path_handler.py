"""
file_path_handler.py

Dedicated module for handling climate data file paths, validation, and discovery.
Centralizes all file system operations and path management for the climate processing pipeline.
"""

import os
import glob
import platform
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class FilePathConfig:
    """Configuration class for file paths and naming patterns."""
    external_drive_path: str
    variables: List[str]
    scenarios: List[str]
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self.external_drive_path = Path(self.external_drive_path).resolve()
        
        # Validate variables
        valid_variables = ['tas', 'tasmax', 'tasmin', 'pr']
        invalid_vars = set(self.variables) - set(valid_variables)
        if invalid_vars:
            raise ValueError(f"Invalid variables specified: {invalid_vars}. "
                           f"Valid options: {valid_variables}")
        
        # Validate scenarios
        valid_scenarios = ['historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585']
        invalid_scenarios = set(self.scenarios) - set(valid_scenarios)
        if invalid_scenarios:
            raise ValueError(f"Invalid scenarios specified: {invalid_scenarios}. "
                           f"Valid options: {valid_scenarios}")

class ClimateFileHandler:
    """Handles all file path operations for climate data processing."""
    
    def __init__(self, config: FilePathConfig):
        """
        Initialize the file handler with configuration.
        
        Args:
            config (FilePathConfig): Configuration object with paths and patterns
        """
        self.config = config
        self.base_path = Path(config.external_drive_path)
        
        # File naming templates
        self._file_templates = {
            'historical': '{var}_day_NorESM2-LM_historical_r1i1p1f1_gn_{year}.nc',
            'future': '{var}_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}.nc'
        }
        
        # Data availability ranges
        self._data_availability = {
            'historical': {'start': 1850, 'end': 2014},
            'ssp126': {'start': 2015, 'end': 2100},
            'ssp245': {'start': 2015, 'end': 2100},
            'ssp370': {'start': 2015, 'end': 2100},
            'ssp585': {'start': 2015, 'end': 2100}
        }
    
    def get_file_pattern(self, variable: str, scenario: str) -> str:
        """
        Generate file glob pattern for a specific variable and scenario.
        
        Args:
            variable (str): Climate variable (tas, tasmax, tasmin, pr)
            scenario (str): Climate scenario (historical, ssp126, etc.)
            
        Returns:
            str: Glob pattern for matching files
        """
        if scenario == 'historical':
            pattern = self._file_templates['historical'].format(var=variable, year='*')
            # Files are organized in subdirectories: {variable}/{scenario}/
            return str(self.base_path / variable / scenario / pattern)
        else:
            pattern = self._file_templates['future'].format(var=variable, scenario=scenario, year='*')
            # Files are organized in subdirectories: {variable}/{scenario}/
            return str(self.base_path / variable / scenario / pattern)
    
    def get_all_file_patterns(self) -> Dict[str, Dict[str, str]]:
        """
        Generate all file patterns for configured variables and scenarios.
        
        Returns:
            Dict[str, Dict[str, str]]: Nested dict of scenario -> variable -> pattern
        """
        patterns = {}
        
        for scenario in self.config.scenarios:
            patterns[scenario] = {}
            for variable in self.config.variables:
                patterns[scenario][variable] = self.get_file_pattern(variable, scenario)
        
        return patterns
    
    def extract_year_from_filename(self, filename: str) -> Optional[int]:
        """
        Extract year from climate data filename.
        
        Args:
            filename (str): Filename to parse
            
        Returns:
            Optional[int]: Extracted year or None if not found
        """
        base = os.path.basename(filename)
        
        # Split by underscores and look for year
        parts = base.split('_')
        
        # Look for year in parts - should be the last part before .nc
        for part in reversed(parts):
            # Remove .nc extension if present
            if part.endswith('.nc'):
                part = part[:-3]
            
            # Check if this part is a 4-digit year
            if len(part) == 4 and part.isdigit():
                year = int(part)
                # Validate year range (climate data typically 1850-2100)
                if 1850 <= year <= 2100:
                    return year
        
        logger.warning(f"Could not extract year from {filename}")
        return None
    
    def get_files_for_period(self, variable: str, scenario: str, 
                           start_year: int, end_year: int) -> List[str]:
        """
        Get all files for a specific variable, scenario, and year range.
        
        Args:
            variable (str): Climate variable
            scenario (str): Climate scenario
            start_year (int): Start year (inclusive)
            end_year (int): End year (inclusive)
            
        Returns:
            List[str]: Sorted list of matching file paths
        """
        pattern = self.get_file_pattern(variable, scenario)
        logger.debug(f"Looking for files matching pattern: {pattern}")
        logger.debug(f"For years: {start_year} to {end_year}")
        
        all_files = glob.glob(pattern)
        logger.debug(f"Found {len(all_files)} total files for {variable} {scenario}")
        
        period_files = []
        for file_path in all_files:
            year = self.extract_year_from_filename(file_path)
            if year and start_year <= year <= end_year:
                period_files.append(file_path)
        
        if not period_files:
            logger.warning(f"No files found for {variable} {scenario} "
                         f"in years {start_year}-{end_year}")
        else:
            logger.info(f"Found {len(period_files)} files for {variable} {scenario} "
                       f"in years {start_year}-{end_year}")
        
        return sorted(period_files)
    
    def validate_data_availability(self, scenario: str, start_year: int, end_year: int) -> bool:
        """
        Validate that requested years are within expected data availability.
        
        Args:
            scenario (str): Climate scenario
            start_year (int): Start year
            end_year (int): End year
            
        Returns:
            bool: True if years are within expected range
        """
        if scenario not in self._data_availability:
            logger.warning(f"Unknown scenario: {scenario}")
            return False
        
        data_range = self._data_availability[scenario]
        
        if start_year < data_range['start'] or end_year > data_range['end']:
            logger.warning(f"Requested years {start_year}-{end_year} outside "
                         f"expected range for {scenario}: "
                         f"{data_range['start']}-{data_range['end']}")
            return False
        
        return True
    
    def discover_available_files(self) -> Dict[str, Dict[str, List[Tuple[int, str]]]]:
        """
        Discover all available files and their years for each variable/scenario combination.
        
        Returns:
            Dict[str, Dict[str, List[Tuple[int, str]]]]: 
            scenario -> variable -> [(year, filepath), ...]
        """
        discovered = {}
        
        for scenario in self.config.scenarios:
            discovered[scenario] = {}
            
            for variable in self.config.variables:
                pattern = self.get_file_pattern(variable, scenario)
                all_files = glob.glob(pattern)
                
                file_years = []
                for file_path in all_files:
                    year = self.extract_year_from_filename(file_path)
                    if year:
                        file_years.append((year, file_path))
                
                # Sort by year
                file_years.sort(key=lambda x: x[0])
                discovered[scenario][variable] = file_years
                
                if file_years:
                    years = [year for year, _ in file_years]
                    logger.info(f"Discovered {len(file_years)} files for {variable} {scenario}: "
                              f"{min(years)}-{max(years)}")
                else:
                    logger.warning(f"No files discovered for {variable} {scenario}")
        
        return discovered
    
    def validate_base_path(self) -> bool:
        """
        Validate that the base data path exists and is accessible.
        
        Returns:
            bool: True if path is valid and accessible
        """
        if not self.base_path.exists():
            logger.error(f"Base data path does not exist: {self.base_path}")
            self._suggest_alternative_paths()
            return False
        
        if not self.base_path.is_dir():
            logger.error(f"Base data path is not a directory: {self.base_path}")
            return False
        
        # Test read access
        try:
            list(self.base_path.iterdir())
        except PermissionError:
            logger.error(f"No read access to base data path: {self.base_path}")
            return False
        
        logger.info(f"✓ Base data path validated: {self.base_path}")
        return True
    
    def _suggest_alternative_paths(self) -> None:
        """Suggest alternative paths based on the current system."""
        system = platform.system()
        logger.info(f"Detected system: {system}")
        
        if system == "Darwin":  # macOS
            volumes_path = Path("/Volumes")
            if volumes_path.exists():
                volumes = [d for d in volumes_path.iterdir() if d.is_dir()]
                logger.info(f"Available volumes in {volumes_path}:")
                for vol in volumes:
                    logger.info(f"  {vol}")
        
        elif system == "Windows":
            import string
            available_drives = [f'{d}:' for d in string.ascii_uppercase 
                              if Path(f'{d}:').exists()]
            logger.info("Available drives:")
            for drive in available_drives:
                logger.info(f"  {drive}")
        
        elif system == "Linux":
            media_paths = [Path("/media"), Path("/mnt")]
            for media_path in media_paths:
                if media_path.exists():
                    try:
                        mounts = [d for d in media_path.iterdir() if d.is_dir()]
                        if mounts:
                            logger.info(f"Available mounts in {media_path}:")
                            for mount in mounts:
                                logger.info(f"  {mount}")
                    except PermissionError:
                        logger.warning(f"Permission denied accessing {media_path}")
    
    def get_expected_file_structure_info(self) -> str:
        """
        Generate informative text about expected file structure.
        
        Returns:
            str: Formatted description of expected file structure
        """
        info = [
            "Expected directory structure and file naming patterns:",
            "",
            f"{self.base_path}/",
            "├── tas/",
            "│   ├── historical/",
            "│   │   ├── tas_day_NorESM2-LM_historical_r1i1p1f1_gn_1850.nc",
            "│   │   ├── tas_day_NorESM2-LM_historical_r1i1p1f1_gn_1851.nc",
            "│   │   └── ...",
            "│   ├── ssp126/",
            "│   │   ├── tas_day_NorESM2-LM_ssp126_r1i1p1f1_gn_2015.nc",
            "│   │   └── ...",
            "│   └── ...",
            "├── tasmax/",
            "│   ├── historical/",
            "│   ├── ssp126/",
            "│   └── ...",
            "├── tasmin/",
            "│   ├── historical/",
            "│   ├── ssp126/",
            "│   └── ...",
            "└── pr/",
            "    ├── historical/",
            "    ├── ssp126/",
            "    └── ...",
            "",
            "File naming patterns:",
            "Historical: {var}_day_NorESM2-LM_historical_r1i1p1f1_gn_{year}.nc",
            "Future: {var}_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}.nc",
            "",
            "Where:",
            f"  {{var}} = {', '.join(self.config.variables)}",
            f"  {{scenario}} = {', '.join([s for s in self.config.scenarios if s != 'historical'])}",
            "  {year} = 1850-2014 for historical, 2015-2100 for future scenarios"
        ]
        
        return "\n".join(info)

def create_file_handler(external_drive_path: str, 
                       variables: List[str] = None, 
                       scenarios: List[str] = None) -> ClimateFileHandler:
    """
    Convenience function to create a ClimateFileHandler with sensible defaults.
    
    Args:
        external_drive_path (str): Path to external drive containing climate data
        variables (List[str], optional): Variables to process. 
                                       Defaults to ['tas', 'tasmax', 'tasmin', 'pr']
        scenarios (List[str], optional): Scenarios to process. 
                                       Defaults to ['historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585']
    
    Returns:
        ClimateFileHandler: Configured file handler instance
    """
    if variables is None:
        variables = ['tas', 'tasmax', 'tasmin', 'pr']
    
    if scenarios is None:
        scenarios = ['historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585']
    
    config = FilePathConfig(
        external_drive_path=external_drive_path,
        variables=variables,
        scenarios=scenarios
    )
    
    return ClimateFileHandler(config)