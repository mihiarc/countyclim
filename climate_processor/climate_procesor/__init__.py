# climate_processor/__init__.py
"""
Climate Data Processor Package

A modular, efficient climate data processing pipeline for calculating
30-year climate normals from daily NetCDF files.
"""

__version__ = "2.0.0"
__author__ = "Climate Data Processing Team"

from .data_processor import ClimateDataProcessor
from .config import load_configuration
from .file_handler import create_file_handler

__all__ = ['ClimateDataProcessor', 'load_configuration', 'create_file_handler']