import pandas as pd
import csv
import requests
from io import StringIO
from typing import Union, Optional, Dict, Any
import os

class CSVParser:
    def __init__(self):
        """Initialize the CSV Parser with default settings."""
        self.data = None
        self.source_type = None

    def read_csv(self, 
                 source: str,
                 source_type: str = 'local',
                 delimiter: str = ',',
                 encoding: str = 'utf-8',
                 **kwargs) -> pd.DataFrame:
        """
        Read CSV data from various sources.
        
        Args:
            source (str): Path to CSV file or URL
            source_type (str): Type of source ('local', 'url', or 'string')
            delimiter (str): CSV delimiter character
            encoding (str): File encoding
            **kwargs: Additional arguments to pass to pandas read_csv
        
        Returns:
            pd.DataFrame: Parsed CSV data
        """
        try:
            if source_type == 'local':
                if not os.path.exists(source):
                    raise FileNotFoundError(f"File not found: {source}")
                self.data = pd.read_csv(source, delimiter=delimiter, encoding=encoding, **kwargs)
            
            elif source_type == 'url':
                response = requests.get(source)
                response.raise_for_status()
                csv_content = StringIO(response.text)
                self.data = pd.read_csv(csv_content, delimiter=delimiter, encoding=encoding, **kwargs)
            
            elif source_type == 'string':
                csv_content = StringIO(source)
                self.data = pd.read_csv(csv_content, delimiter=delimiter, encoding=encoding, **kwargs)
            
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            self.source_type = source_type
            return self.data
        
        except Exception as e:
            raise Exception(f"Error reading CSV: {str(e)}")

    def get_basic_stats(self) -> Dict[str, Any]:
        """
        Get basic statistics about the CSV data.
        
        Returns:
            Dict containing basic statistics
        """
        if self.data is None:
            raise ValueError("No data loaded. Call read_csv first.")
        
        return {
            'rows': len(self.data),
            'columns': len(self.data.columns),
            'column_names': list(self.data.columns),
            'data_types': self.data.dtypes.to_dict(),
            'missing_values': self.data.isnull().sum().to_dict()
        }

    def save_csv(self, 
                 output_path: str,
                 delimiter: str = ',',
                 encoding: str = 'utf-8',
                 **kwargs) -> None:
        """
        Save the parsed data to a CSV file.
        
        Args:
            output_path (str): Path to save the CSV file
            delimiter (str): CSV delimiter character
            encoding (str): File encoding
            **kwargs: Additional arguments to pass to pandas to_csv
        """
        if self.data is None:
            raise ValueError("No data loaded. Call read_csv first.")
        
        self.data.to_csv(output_path, sep=delimiter, encoding=encoding, **kwargs)

# Example usage
if __name__ == "__main__":
    parser = CSVParser()
    
    # Example 1: Reading from a local file
    try:
        df = parser.read_csv('example.csv', source_type='local')
        print("Local file stats:", parser.get_basic_stats())
    except Exception as e:
        print(f"Error reading local file: {e}")
    
    # Example 2: Reading from a URL
    try:
        url = "https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv"
        df = parser.read_csv(url, source_type='url')
        print("URL file stats:", parser.get_basic_stats())
    except Exception as e:
        print(f"Error reading from URL: {e}")
    
    # Example 3: Reading from a string
    try:
        csv_string = "name,age,city\nJohn,30,New York\nAlice,25,London"
        df = parser.read_csv(csv_string, source_type='string')
        print("String data stats:", parser.get_basic_stats())
    except Exception as e:
        print(f"Error reading from string: {e}") 