"""
Climate period generation and management.

This module handles the generation of 30-year climate periods
for different scenarios.
"""

from typing import List, Dict
from dataclasses import dataclass


@dataclass
class ClimatePeriod:
    """Data class representing a climate period."""
    start_year: int
    end_year: int
    target_year: int
    period_name: str
    
    @property
    def length(self) -> int:
        """Get the length of the period in years."""
        return self.end_year - self.start_year + 1
    
    def __str__(self) -> str:
        """String representation of the period."""
        return f"{self.period_name} ({self.start_year}-{self.end_year}, target: {self.target_year})"


class PeriodGenerator:
    """Generator for climate periods based on scenarios."""
    
    def __init__(self, data_availability: Dict[str, Dict[str, int]]):
        """
        Initialize period generator.
        
        Args:
            data_availability: Dictionary mapping scenarios to start/end years
        """
        self.data_availability = data_availability
        self.min_years_required = 20  # Minimum years needed for a valid period
        self.period_length = 30  # Standard climate normal period
    
    def generate_periods(self, scenario: str) -> List[ClimatePeriod]:
        """
        Generate climate periods based on scenario.
        
        Each period represents a 30-year window for calculating climate normals.
        Each year's climate is based on the preceding 30 years ending in that year.
        
        Args:
            scenario: Climate scenario ('historical' or projection scenario)
            
        Returns:
            List of ClimatePeriod objects
        """
        periods = []
        
        # Get data availability for this scenario
        if scenario not in self.data_availability:
            print(f"Warning: No data availability info for scenario {scenario}, using historical defaults")
            data_start = 1950
            data_end = 2014
        else:
            data_start = self.data_availability[scenario]['start']
            data_end = self.data_availability[scenario]['end']
        
        if scenario == 'historical':
            # Historical: 1980-2014 (35 years of annual climate measures)
            # Each year's climate is based on the preceding 30 years
            for target_year in range(1980, 2015):
                end_year = target_year      # Climate period ends in the target year
                start_year = target_year - self.period_length + 1  # 30 years total
                
                # Ensure we don't go outside available data
                start_year = max(start_year, data_start)
                end_year = min(end_year, data_end)
                
                # Only include if we have at least min_years_required years of data
                if end_year - start_year + 1 >= self.min_years_required:
                    period_name = f"climate_{target_year}"
                    periods.append(ClimatePeriod(start_year, end_year, target_year, period_name))
        
        else:
            # Future scenarios: 2015-2100 (86 years of annual climate measures)
            # Each year's climate is based on the preceding 30 years
            for target_year in range(2015, 2101):
                end_year = target_year      # Climate period ends in the target year
                start_year = target_year - self.period_length + 1  # 30 years total
                
                # Ensure we don't go outside available data
                start_year = max(start_year, data_start)
                end_year = min(end_year, data_end)
                
                # Only include if we have at least min_years_required years of data
                if end_year - start_year + 1 >= self.min_years_required:
                    period_name = f"climate_{target_year}"
                    periods.append(ClimatePeriod(start_year, end_year, target_year, period_name))
        
        self._log_period_summary(scenario, periods, data_start, data_end)
        
        return periods
    
    def _log_period_summary(self, scenario: str, periods: List[ClimatePeriod], 
                           data_start: int, data_end: int) -> None:
        """Log summary information about generated periods."""
        print(f"Generated {len(periods)} climate periods for {scenario}")
        print(f"  Data availability: {data_start}-{data_end}")
        
        if periods:
            first_period = periods[0]
            last_period = periods[-1]
            print(f"  Climate periods: {first_period.target_year} to {last_period.target_year} (target years)")
            print(f"  Example: Climate for {first_period.target_year} based on "
                  f"{first_period.start_year}-{first_period.end_year} ({first_period.length} years)")
    
    def get_period_by_year(self, periods: List[ClimatePeriod], target_year: int) -> ClimatePeriod:
        """
        Get the climate period for a specific target year.
        
        Args:
            periods: List of climate periods
            target_year: The target year to find
            
        Returns:
            ClimatePeriod for the target year
            
        Raises:
            ValueError: If target year not found
        """
        for period in periods:
            if period.target_year == target_year:
                return period
        
        raise ValueError(f"No climate period found for target year {target_year}") 