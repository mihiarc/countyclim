"""
Main Data Processing Module

Orchestrates the climate data processing pipeline using modular components.
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple
import xarray as xr
import dask
from tqdm import tqdm
import gc

from .file_handler import create_file_handler
from .regions import REGION_BOUNDS, generate_climate_periods
from .dask_utils import setup_dask_client
from .climate_calculations import process_period_region_optimized
from .io_utils import save_region_dataset_optimized

logger = logging.getLogger(__name__)

# Memory logging function - simplified version to avoid import issues
def log_memory_usage(stage: str = "") -> None:
    """Log current memory usage."""
    try:
        import psutil
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

class ClimateDataProcessor:
    """
    Main climate data processing orchestrator.
    
    Coordinates all aspects of the climate data processing pipeline including
    file discovery, Dask setup, computation scheduling, and output generation.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor with configuration.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.file_handler = None
        self.client = None
        self.cluster = None
        
        # Create output directory
        import os
        os.makedirs(self.config['output_dir'], exist_ok=True)
    
    def setup(self) -> bool:
        """
        Set up the processor components.
        
        Returns:
            True if setup successful, False otherwise
        """
        try:
            # Initialize file handler
            logger.info("Initializing file handler...")
            self.file_handler = create_file_handler(
                external_drive_path=self.config['external_drive_path'],
                variables=self.config['variables'],
                scenarios=[self.config['active_scenario']]
            )
            
            # Validate file paths
            if not self.file_handler.validate_base_path():
                logger.error("File path validation failed")
                logger.info(self.file_handler.get_expected_file_structure_info())
                return False
            
            # Set up Dask client
            logger.info("Setting up Dask client...")
            self.client, self.cluster = setup_dask_client(self.config)
            
            log_memory_usage("after setup")
            return True
            
        except Exception as e:
            logger.error(f"Setup failed: {e}", exc_info=True)
            return False
    
    def discover_files(self) -> Dict:
        """
        Discover available files.
        
        Returns:
            Dictionary of available files by scenario and variable
        """
        logger.info("Discovering available files...")
        available_files = self.file_handler.discover_available_files()
        log_memory_usage("after file discovery")
        
        return available_files
    
    def generate_processing_plan(self, available_files: Dict) -> Tuple[List, List]:
        """
        Generate the processing plan.
        
        Args:
            available_files: Dictionary of available files
        
        Returns:
            Tuple of (climate_periods, processing_variables)
        """
        scenario = self.config['active_scenario']
        
        # Generate climate periods
        data_availability = self.file_handler._data_availability
        climate_periods = generate_climate_periods(scenario, data_availability)
        
        if not climate_periods:
            logger.error(f"No climate periods generated for scenario {scenario}")
            return [], []
        
        # Get available variables for this scenario
        available_vars = list(available_files.get(scenario, {}).keys())
        processing_vars = [var for var in self.config['variables'] if var in available_vars]
        
        if not processing_vars:
            logger.error(f"No variables available for processing in scenario {scenario}")
            logger.info(f"Available variables: {available_vars}")
            return [], []
        
        logger.info(f"Processing variables: {processing_vars}")
        logger.info(f"Climate periods: {len(climate_periods)}")
        
        return climate_periods, processing_vars
    
    def create_computation_tasks(self, climate_periods: List, processing_vars: List) -> List:
        """
        Create lazy computation tasks.
        
        Args:
            climate_periods: List of climate periods to process
            processing_vars: List of variables to process
        
        Returns:
            List of computation tasks
        """
        logger.info("Creating lazy computation graph...")
        log_memory_usage("before creating computation graph")
        
        computation_tasks = []
        
        # Process each variable and period to build lazy computation graph
        for var_name in processing_vars:
            logger.info(f"Setting up lazy computations for {var_name}...")
            
            # Process each climate period
            for period in climate_periods:
                start_year, end_year, target_year, period_name = period
                
                # Process each region
                for region_key in REGION_BOUNDS.keys():
                    # Create lazy computation for this combination
                    lazy_result = dask.delayed(process_period_region_optimized)(
                        period, var_name, self.file_handler, region_key
                    )
                    
                    # Store the task for batch computation
                    task_key = f"{region_key}_{var_name}_{target_year}"
                    computation_tasks.append((task_key, lazy_result))
        
        logger.info(f"Created {len(computation_tasks)} lazy computation tasks")
        log_memory_usage("after creating computation graph")
        
        return computation_tasks
    
    def execute_computation_batches(self, computation_tasks: List) -> Dict:
        """
        Execute computation tasks in batches.
        
        Args:
            computation_tasks: List of computation tasks
        
        Returns:
            Dictionary of results organized by region
        """
        region_lazy_datasets = {}
        batch_size = self.config['batch_size']
        
        logger.info(f"Computing tasks in batches of {batch_size}...")
        
        total_batches = (len(computation_tasks) + batch_size - 1) // batch_size
        
        # Use tqdm for progress tracking
        for i in tqdm(range(0, len(computation_tasks), batch_size),
                     desc="Processing batches",
                     total=total_batches):
            batch = computation_tasks[i:i + batch_size]
            batch_keys = [key for key, _ in batch]
            batch_tasks = [task for _, task in batch]
            
            current_batch = i // batch_size + 1
            logger.info(f"Computing batch {current_batch}/{total_batches}")
            logger.debug(f"Batch contains tasks: {batch_keys[:3]}{'...' if len(batch_keys) > 3 else ''}")
            
            log_memory_usage(f"before batch {current_batch}")
            
            # Compute this batch
            batch_results = dask.compute(*batch_tasks)
            
            log_memory_usage(f"after computing batch {current_batch}")
            
            # Organize results by region
            for (task_key, _), result in zip(batch, batch_results):
                if result is not None:
                    region_key, var_name, target_year = task_key.split('_', 2)
                    
                    if region_key not in region_lazy_datasets:
                        region_lazy_datasets[region_key] = {}
                    
                    var_id = f"{var_name}_{target_year}"
                    region_lazy_datasets[region_key][var_id] = result
                    
                    logger.debug(f"Added {var_name} for target year {target_year} "
                               f"in {REGION_BOUNDS[region_key]['name']}")
            
            # Trigger garbage collection between batches
            gc.collect()
            log_memory_usage(f"after cleanup batch {current_batch}")
        
        return region_lazy_datasets
    
    def save_results(self, region_datasets: Dict) -> List[str]:
        """
        Save processed datasets to files.
        
        Args:
            region_datasets: Dictionary of datasets by region
        
        Returns:
            List of saved file paths
        """
        logger.info("Creating final datasets and saving...")
        log_memory_usage("before creating final datasets")
        
        saved_files = []
        scenario = self.config['active_scenario']
        
        for region_key, var_dict in region_datasets.items():
            if var_dict:  # Only process regions with data
                region_name = REGION_BOUNDS[region_key]['name']
                logger.info(f"Creating dataset for {region_name} with {len(var_dict)} variables")
                
                log_memory_usage(f"before creating {region_name} dataset")
                
                # Create xarray Dataset
                region_dataset = xr.Dataset(var_dict)
                
                # Save with optimized I/O
                output_file = save_region_dataset_optimized(
                    region_key, region_dataset, scenario, self.config
                )
                saved_files.append(output_file)
                
                # Clean up memory
                del region_dataset
                gc.collect()
                log_memory_usage(f"after saving {region_name}")
            else:
                logger.warning(f"No data collected for region {REGION_BOUNDS[region_key]['name']}")
        
        return saved_files
    
    def cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up resources...")
        
        try:
            if self.client:
                self.client.restart()
                self.client.close()
            if self.cluster:
                self.cluster.close()
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
        
        # Final memory cleanup
        gc.collect()
        log_memory_usage("after cleanup")
    
    def run(self) -> bool:
        """
        Run the complete processing pipeline.
        
        Returns:
            True if processing successful, False otherwise
        """
        start_time = datetime.now()
        logger.info(f"Starting climate data processing at {start_time}")
        log_memory_usage("startup")
        
        try:
            # Setup
            if not self.setup():
                return False
            
            # Discover files
            available_files = self.discover_files()
            
            # Generate processing plan
            climate_periods, processing_vars = self.generate_processing_plan(available_files)
            if not climate_periods or not processing_vars:
                return False
            
            # Create computation tasks
            computation_tasks = self.create_computation_tasks(climate_periods, processing_vars)
            if not computation_tasks:
                logger.error("No computation tasks created")
                return False
            
            # Execute computations
            region_datasets = self.execute_computation_batches(computation_tasks)
            
            # Save results
            saved_files = self.save_results(region_datasets)
            
            # Final summary
            end_time = datetime.now()
            runtime = end_time - start_time
            
            logger.info("Processing complete!")
            logger.info(f"Total runtime: {runtime}")
            logger.info(f"Saved {len(saved_files)} output files:")
            for file_path in saved_files:
                logger.info(f"  {file_path}")
            
            # Performance statistics
            total_tasks = len(computation_tasks)
            tasks_per_minute = total_tasks / (runtime.total_seconds() / 60)
            logger.info(f"Processing rate: {tasks_per_minute:.1f} tasks/minute")
            
            log_memory_usage("final")
            
            return True
            
        except Exception as e:
            logger.error(f"Processing failed: {e}", exc_info=True)
            log_memory_usage("error state")
            return False
        
        finally:
            self.cleanup()