import requests
import json
import os
from datetime import datetime, timedelta
import time
from pathlib import Path
from utils.logger import setup_logger

# Initialize logger for this module
logger = setup_logger("weather_collector")

class WeatherDataCollector:
    """
    A class that collects weather data for Seattle from the National Weather Service API.
    Main functionalities:
    1. Fetches weather data from multiple sources (forecasts, observations, alerts)
    2. Processes and saves the data in organized directories
    3. Handles API interactions with error handling
    4. Manages data retention and scheduled collection
    """
    def __init__(self, retention_days=30):
        # API configuration
        self.base_url = "https://api.weather.gov"  # National Weather Service API endpoint
        self.headers = {
            "User-Agent": "SeattleMetroWeatherAnalysis/1.0",  # Identify our application to the API
            "Accept": "application/json"  # Specify we want JSON responses
        }
        
        # Data storage and location settings
        self.data_dir = Path(__file__).parent.parent / "raw_data"  # Base directory for storing data
        self.seattle_coords = "47.6062,-122.3321"  # Latitude,Longitude for Seattle
        self.retention_days = retention_days  # How long to keep historical data
        
    def create_data_directories(self):
        """
        Create organized directory structure for different types of weather data.
        Creates separate folders for observations, forecasts, alerts, and station data.
        """
        directories = ['observations', 'forecasts', 'alerts', 'stations']
        for dir_name in directories:
            path = os.path.join(self.data_dir, dir_name)
            os.makedirs(path, exist_ok=True)  # Create directory if it doesn't exist
            logger.info(f"Created/verified directory: {dir_name}")

    def cleanup_old_data(self):
        """
        Remove data files that are older than the retention period.
        This prevents unlimited accumulation of historical data.
        """
        logger.info(f"Starting cleanup of files older than {self.retention_days} days")
        for category in ['observations', 'forecasts', 'alerts', 'stations']:
            category_path = os.path.join(self.data_dir, category)
            if not os.path.exists(category_path):
                continue
                
            # Check each file in the category directory
            for file in os.listdir(category_path):
                file_path = os.path.join(category_path, file)
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                # Remove if file is older than retention period
                if datetime.now() - file_time > timedelta(days=self.retention_days):
                    try:
                        os.remove(file_path)
                        logger.info(f"Removed old file: {file}")
                    except Exception as e:
                        logger.error(f"Error removing file {file}: {str(e)}")

    def save_json_data(self, data, category, filename):
        """
        Save collected data as JSON file with timestamp in filename.
        
        Args:
            data: The data to save
            category: Type of data (observations/forecasts/alerts/stations)
            filename: Base name for the file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(self.data_dir, category, f"{filename}_{timestamp}.json")
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=4)
            logger.info(f"Successfully saved data to {filepath}")
        except Exception as e:
            logger.error(f"Error saving data to {filepath}: {str(e)}")

    def get_seattle_point_data(self):
        """Get Seattle grid point and station information"""
        response = requests.get(f"{self.base_url}/points/{self.seattle_coords}", 
                              headers=self.headers)
        if response.status_code == 200:
            return response.json()
        return None

    def fetch_station_observations(self, station_id):
        """Fetch latest observations from a weather station"""
        # Add query parameters
        params = {
            'limit': 100,  # Get last 100 observations
            'start': (datetime.now() - timedelta(days=1)).isoformat(),  # Last 24 hours
            'end': datetime.now().isoformat()
        }
        
        try:
            response = requests.get(
                f"{self.base_url}/stations/{station_id}/observations",
                headers=self.headers,
                params=params
            )
            
            if response.status_code == 200:
                data = response.json()
                if not data['features']:
                    print(f"No observations found for station {station_id}")
                    return None
                return data
            else:
                print(f"Error fetching data for station {station_id}: {response.status_code}")
                return None
            
        except requests.exceptions.RequestException as e:
            print(f"Request failed for station {station_id}: {str(e)}")
            return None

    def fetch_hourly_forecast(self, grid_id, grid_x, grid_y):
        """Fetch hourly forecast data"""
        response = requests.get(
            f"{self.base_url}/gridpoints/{grid_id}/{grid_x},{grid_y}/forecast/hourly",
            headers=self.headers
        )
        if response.status_code == 200:
            return response.json()
        return None

    def fetch_alerts(self):
        """Fetch active weather alerts for the area"""
        response = requests.get(
            f"{self.base_url}/alerts/active/area/WA",
            headers=self.headers
        )
        if response.status_code == 200:
            return response.json()
        return None

    def fetch_detailed_weather_data(self, station_id):
        """Fetch detailed weather data for a station"""
        try:
            # Construct the URL for station observations
            url = f"{self.base_url}/stations/{station_id}/observations"
            print(f"Fetching from URL: {url}")
            
            # Format dates in ISO 8601 format with timezone
            end_time = datetime.now().replace(microsecond=0).isoformat() + 'Z'
            start_time = (datetime.now() - timedelta(days=1)).replace(microsecond=0).isoformat() + 'Z'
            
            # Add query parameters with properly formatted dates
            params = {
                'limit': 100,  # Get last 100 observations
                'start': start_time,
                'end': end_time
            }
            
            print(f"Request parameters: {params}")  # Debug print
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('features'):
                    return self.process_weather_data(data, station_id)
                else:
                    print(f"No observations found for station {station_id}")
                    return None
            else:
                print(f"Error fetching data for station {station_id}: {response.status_code}")
                print(f"Response content: {response.text[:200]}...")  # Print first 200 chars of response
                return None
            
        except Exception as e:
            print(f"Exception while fetching data for station {station_id}: {str(e)}")
            return None

    def process_weather_data(self, data, station_id):
        """Process the weather data into a useful format"""
        processed_data = {
            'station_id': station_id,
            'timestamp': datetime.now().isoformat(),
            'observations': []
        }
        
        for feature in data['features']:
            props = feature.get('properties', {})
            observation = {
                'timestamp': props.get('timestamp'),
                'temperature': self.extract_value(props.get('temperature')),
                'windSpeed': self.extract_value(props.get('windSpeed')),
                'windDirection': self.extract_value(props.get('windDirection')),
                'visibility': self.extract_value(props.get('visibility')),
                'precipitation': self.extract_value(props.get('precipitationLastHour')),
                'relativeHumidity': self.extract_value(props.get('relativeHumidity')),
                'weatherCondition': props.get('textDescription')
            }
            processed_data['observations'].append(observation)
        
        return processed_data

    def extract_value(self, measurement):
        """Extract value from measurement object"""
        if isinstance(measurement, dict):
            return {
                'value': measurement.get('value'),
                'unit': measurement.get('unitCode')
            }
        return None

    def fetch_all_data(self):
        """
        Main workflow:
        1. Creates necessary directories
        2. Gets Seattle point data
        3. Fetches forecasts
        4. Collects data from all nearby weather stations
        5. Gets active weather alerts
        6. Saves everything in organized JSON files
        """
        logger.info("Starting data collection process")
        
        try:
            self.create_data_directories()
            self.cleanup_old_data()
            
            # Get Seattle point data
            logger.info("Fetching Seattle point data")
            point_data = self.get_seattle_point_data()
            
            if not point_data:
                logger.error("Failed to fetch Seattle point data")
                return
                
            self.save_json_data(point_data, 'stations', 'seattle_point_data')
            
            try:
                # Extract grid and station information
                properties = point_data['properties']
                grid_id = properties['gridId']
                grid_x = properties['gridX']
                grid_y = properties['gridY']
                
                logger.info(f"Grid Information - ID: {grid_id}, X: {grid_x}, Y: {grid_y}")
                
                # Fetch and save hourly forecast
                logger.info("Fetching hourly forecast")
                forecast_data = self.fetch_hourly_forecast(grid_id, grid_x, grid_y)
                if forecast_data:
                    self.save_json_data(forecast_data, 'forecasts', 'hourly_forecast')
                
                # Fetch observation stations
                logger.info("Fetching observation stations data")
                stations_response = requests.get(
                    properties['observationStations'],
                    headers=self.headers
                )
                
                if stations_response.status_code == 200:
                    stations_data = stations_response.json()
                    stations = stations_data.get('features', [])
                    logger.info(f"Found {len(stations)} observation stations")
                    
                    for station in stations:
                        station_id = station.get('properties', {}).get('stationIdentifier')
                        if station_id:
                            logger.info(f"Processing station: {station_id}")
                            observations = self.fetch_detailed_weather_data(station_id)
                            if observations:
                                self.save_json_data(observations, 'observations', f'station_{station_id}')
                
                # Fetch and save alerts
                logger.info("Fetching weather alerts")
                alerts = self.fetch_alerts()
                if alerts:
                    self.save_json_data(alerts, 'alerts', 'alerts')
                
            except Exception as e:
                logger.error(f"Error in data collection process: {str(e)}", exc_info=True)
                
        except Exception as e:
            logger.error(f"Critical error in fetch_all_data: {str(e)}", exc_info=True)

    def scheduled_collection(self, interval_hours=1):
        """
        Run continuous data collection at specified intervals.
        
        Args:
            interval_hours: Hours to wait between collections
        """
        logger.info(f"Starting scheduled collection every {interval_hours} hours")
        
        while True:
            try:
                # Record start time to calculate actual sleep needed
                collection_start = datetime.now()
                logger.info(f"Starting scheduled collection at {collection_start}")
                
                # Perform data collection and cleanup
                self.fetch_all_data()
                self.cleanup_old_data()
                
                # Calculate sleep time accounting for processing duration
                processing_time = (datetime.now() - collection_start).total_seconds()
                sleep_time = max(0, (interval_hours * 3600) - processing_time)
                
                logger.info(f"Waiting {sleep_time/3600:.2f} hours until next collection")
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error in scheduled collection: {str(e)}")
                logger.info("Waiting 5 minutes before retry...")
                time.sleep(300)  # 5 minutes retry delay on error

def main():
    """
    Main entry point with command line argument handling.
    Supports both one-time and scheduled collection modes.
    """
    import argparse
    
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Seattle Weather Data Collector')
    parser.add_argument('--schedule', action='store_true', 
                       help='Run in scheduled mode')
    parser.add_argument('--interval', type=float, default=1.0, 
                       help='Collection interval in hours (default: 1.0)')
    parser.add_argument('--retention', type=int, default=30,
                       help='Data retention period in days (default: 30)')
    
    args = parser.parse_args()
    
    # Initialize collector with specified retention period
    fetcher = WeatherDataCollector(retention_days=args.retention)
    
    # Run in either scheduled or one-time mode
    if args.schedule:
        logger.info(f"Starting scheduled collection every {args.interval} hours")
        fetcher.scheduled_collection(interval_hours=args.interval)
    else:
        logger.info("Running single collection")
        fetcher.fetch_all_data()

if __name__ == "__main__":
    main()