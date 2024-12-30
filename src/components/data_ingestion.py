import requests
import pandas as pd
from pipeline.logger import logger
from pipeline.exception import DataIngestionError

class DataIngestion:
    def __init__(self, api_key, symbol):
        self.api_key = api_key
        self.symbol = symbol
        self.url = 'https://www.alphavantage.co/query'

    def fetch_stock_data(self):
        try:
            logger.info(f"Fetching stock data for symbol: {self.symbol}")
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': self.symbol,
                'apikey': self.api_key,
                'outputsize': 'full',
                'datatype': 'json'
            }
            response = requests.get(self.url, params=params)
            response.raise_for_status()
            data = response.json()

            if 'Time Series (Daily)' not in data:
                raise DataIngestionError("Time Series data not found in response.")

            logger.info("Stock data fetched successfully.")
            return data['Time Series (Daily)']
        except Exception as e:
            logger.error(f"Error in fetching stock data: {str(e)}")
            raise DataIngestionError(str(e))

    def save_data_as_parquet(self, data, filepath):
        try:
            logger.info("Converting JSON data to DataFrame.")
            df = pd.DataFrame.from_dict(data, orient='index')
            df.index.name = 'Date'
            df.reset_index(inplace=True)
            df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            df = df.astype({'Open': 'float', 'High': 'float', 'Low': 'float',
                            'Close': 'float', 'Volume': 'int'})
            df.to_parquet(filepath, engine='pyarrow', index=False)
            logger.info(f"Data saved as parquet at {filepath}.")
            return df
        except Exception as e:
            logger.error(f"Error in saving data as parquet: {str(e)}")
            raise DataIngestionError(str(e))
