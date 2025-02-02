import os
from pathlib import Path

# Base directory of the project
BASE_DIR = Path(__file__).parent.parent

# Landing zone settings
LANDING_ZONE_DIR = BASE_DIR / "landing_zone"
RAW_DATA_DIR = LANDING_ZONE_DIR / "raw_data"

# API settings
WEATHER_API_BASE_URL = "https://api.weather.gov"
WEATHER_API_USER_AGENT = "SeattleMetroWeatherAnalysis/1.0"

# Seattle coordinates
SEATTLE_COORDS = "47.6062,-122.3321"

# Data retention settings (in days)
DATA_RETENTION_DAYS = 30