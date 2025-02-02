import pytest
from landing_zone.collectors.seattle_weather_collector import WeatherDataCollector
from pathlib import Path

# Test to verify the WeatherDataCollector is initialized with correct default values
def test_collector_initialization():
    # Create an instance of the weather collector
    collector = WeatherDataCollector()
    
    # Verify the base URL for the National Weather Service API is set correctly
    assert collector.base_url == "https://api.weather.gov"
    
    # Verify that the data_dir attribute is a proper Path object
    assert isinstance(collector.data_dir, Path)

# Test to ensure the collector creates all necessary data directories
def test_data_directories_creation():
    # Create an instance of the weather collector
    collector = WeatherDataCollector()
    
    # Call the method that creates the directory structure
    collector.create_data_directories()
    
    # Verify that all required subdirectories are created:
    # - observations: for storing weather observation data
    # - forecasts: for storing weather forecast data
    # - alerts: for storing weather alerts/warnings
    # - stations: for storing weather station information
    assert (collector.data_dir / "observations").exists()
    assert (collector.data_dir / "forecasts").exists()
    assert (collector.data_dir / "alerts").exists()
    assert (collector.data_dir / "stations").exists()