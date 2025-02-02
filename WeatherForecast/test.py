# test_weather.py
from WeatherForecast.landing_zone.collectors.seattle_weather_collector import WeatherDataFetcher

def test_fetch():
    fetcher = WeatherDataFetcher()
    print("Starting weather data collection...")
    fetcher.fetch_all_data()
    print("Completed weather data collection")

if __name__ == "__main__":
    test_fetch()