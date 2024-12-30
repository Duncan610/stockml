import pandas as pd
from pipeline.logger import logger
from pipeline.exception import DataTransformationError

class DataTransformation:
    def preprocess_data(self, df):
        try:
            logger.info("Preprocessing data.")
            df['Date'] = pd.to_datetime(df['Date'])
            df.sort_values(by='Date', inplace=True)
            df.fillna(method='ffill', inplace=True)
            logger.info("Data preprocessing completed.")
            return df
        except Exception as e:
            logger.error(f"Error during data preprocessing: {str(e)}")
            raise DataTransformationError(str(e))
