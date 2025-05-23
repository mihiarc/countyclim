"""
Climate Data Processing Pipeline

A modular climate data processing framework for calculating 30-year climate normals
from gridded climate model outputs.
"""

__version__ = "1.0.0"

from .config import ClimateConfig
from .regions import RegionManager, Region
from .periods import PeriodGenerator, ClimatePeriod
from .processors import ClimateProcessor
from .utils import validate_external_drive, list_available_drives

__all__ = [
    "ClimateConfig",
    "RegionManager",
    "Region",
    "PeriodGenerator", 
    "ClimatePeriod",
    "ClimateProcessor",
    "validate_external_drive",
    "list_available_drives"
] 