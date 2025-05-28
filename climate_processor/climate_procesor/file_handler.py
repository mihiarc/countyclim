"""
File Handler Module

Provides file discovery, validation, and management functionality for climate data.
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
        Discover all available files organized by scenario and variable.
        
        Returns:
            Dict: Nested dictionary of scenario -> variable -> [(year, filepath), ...]
        """
        logger.info("Discovering available files...")
        
        available_files = {}
        
        for scenario in self.config.scenarios:
            available_files[scenario] = {}
            
            for variable in self.config.variables:
                pattern = self.get_file_pattern(variable, scenario)
                logger.debug(f"Scanning pattern: {pattern}")
                
                files = glob.glob(pattern)
                year_files = []
                
                for file_path in files:
                    year = self.extract_year_from_filename(file_path)
                    if year:
                        year_files.append((year, file_path))
                
                # Sort by year
                year_files.sort(key=lambda x: x[0])
                available_files[scenario][variable] = year_files
                
                if year_files:
                    years = [y for y, _ in year_files]
                    logger.info(f"Found {len(year_files)} files for {variable} {scenario}: "
                               f"{min(years)}-{max(years)}")
                else:
                    logger.warning(f"No files found for {variable} {scenario}")
        
        return available_files
    
    def validate_base_path(self) -> bool:
        """
        Validate that the base path exists and has the expected structure.
        
        Returns:
            bool: True if base path is valid
        """
        if not self.base_path.exists():
            logger.error(f"Base path does not exist: {self.base_path}")
            self._suggest_alternative_paths()
            return False
        
        if not self.base_path.is_dir():
            logger.error(f"Base path is not a directory: {self.base_path}")
            return False
        
        # Check for expected directory structure
        expected_structure = False
        for variable in ['tas', 'tasmax', 'tasmin', 'pr']:
            var_path = self.base_path / variable
            if var_path.exists():
                for scenario in ['historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585']:
                    scenario_path = var_path / scenario
                    if scenario_path.exists():
                        # Check for at least one NetCDF file
                        nc_files = list(scenario_path.glob('*.nc'))
                        if nc_files:
                            expected_structure = True
                            break
                if expected_structure:
                    break
        
        if not expected_structure:
            logger.warning(f"Expected directory structure not found in: {self.base_path}")
            logger.info(self.get_expected_file_structure_info())
            return False
        
        logger.info(f"✓ Base path validation successful: {self.base_path}")
        return True
    
    def _suggest_alternative_paths(self) -> None:
        """Suggest alternative paths based on the system."""
        system = platform.system()
        
        if system == "Linux":
            suggestions = [
                "/media/*/data/NorESM2-LM",
                "/mnt/*/data/NorESM2-LM", 
                "/home/*/data/NorESM2-LM",
                "/data/NorESM2-LM"
            ]
        elif system == "Darwin":  # macOS
            suggestions = [
                "/Volumes/*/data/NorESM2-LM",
                "/Users/*/data/NorESM2-LM"
            ]
        elif system == "Windows":
            suggestions = [
                "D:\\data\\NorESM2-LM",
                "E:\\data\\NorESM2-LM",
                "C:\\data\\NorESM2-LM"
            ]
        else:
            suggestions = ["/data/NorESM2-LM"]
        
        logger.info("Suggested alternative paths to check:")
        for suggestion in suggestions:
            logger.info(f"  {suggestion}")
        
        # Check if any alternatives exist
        logger.info("Checking for existing data directories...")
        for root in ["/media", "/mnt", "/home"]:
            if os.path.exists(root):
                # Look for directories containing 'NorESM' or climate data patterns
                for item in os.listdir(root):
                    path = os.path.join(root, item)
                    if os.path.isdir(path):
                        # Check subdirectories for climate data
                        try:
                            for subitem in os.listdir(path):
                                if 'NorESM' in subitem or 'climate' in subitem.lower():
                                    logger.info(f"  Found potential data directory: {os.path.join(path, subitem)}")
                        except PermissionError:
                            pass
    
    def get_expected_file_structure_info(self) -> str:
        """
        Get information about the expected file structure.
        
        Returns:
            str: Formatted string describing expected structure
        """
        structure_info = [
            "Expected file structure:",
            f"Base path: {self.base_path}",
            "├── tas/",
            "│   ├── historical/",
            "│   │   ├── tas_day_NorESM2-LM_historical_r1i1p1f1_gn_1850.nc",
            "│   │   ├── tas_day_NorESM2-LM_historical_r1i1p1f1_gn_1851.nc",
            "│   │   └── ...",
            "│   ├── ssp126/",
            "│   │   ├── tas_day_NorESM2-LM_ssp126_r1i1p1f1_gn_2015.nc",
            "│   │   └── ...",
            "│   └── ssp245/",
            "├── tasmax/",
            "├── tasmin/",
            "└── pr/",
            "",
            "Each variable directory should contain scenario subdirectories",
            "with NetCDF files following the naming convention shown above."
        ]
        
        return "\n".join(structure_info)


def create_file_handler(external_drive_path: str, 
                       variables: List[str] = None, 
                       scenarios: List[str] = None) -> ClimateFileHandler:
    """
    Create a file handler instance with the specified configuration.
    
    Args:
        external_drive_path (str): Path to the external drive containing climate data
        variables (List[str], optional): List of climate variables to process
        scenarios (List[str], optional): List of climate scenarios to process
        
    Returns:
        ClimateFileHandler: Configured file handler instance
    """
    if variables is None:
        variables = ['tas', 'tasmax', 'tasmin', 'pr']
    
    if scenarios is None:
        scenarios = ['historical']
    
    config = FilePathConfig(
        external_drive_path=external_drive_path,
        variables=variables,
        scenarios=scenarios
    )
    
    return ClimateFileHandler(config) 