import os
import sys
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon
import json
from typing import Dict, List

from SRC.exception import CustomException
from SRC.logger import logging

from dataclasses import dataclass

@dataclass
class DataProcessing_config:
    preprocessing_data_path=os.path.join('artifacts','ahmedabad_odor_sources_cleaned.csv')
    raw_data_path:str=os.path.join('artifacts','raw.geojson')

class Dataingestion_DataProcessing:
    def __init__(self):
        self.ingestion_processing_config=DataProcessing_config()
    
    def intiatedataingestion(self):
        logging.info("Entered the data ingestion method or component Load a GeoJSON file into a GeoDataFrame.")
        try:
            df=gpd.read_file(r"data\export (2).geojson")
            logging.info("Read the datasets as geopanda dataframe")
            os.makedirs(os.path.dirname(self.ingestion_processing_config.raw_data_path),exist_ok=True)
            df.to_file(self.ingestion_processing_config.raw_data_path, index=False)
            logging.info("Ingestion is completed")
            return(
                self.ingestion_processing_config.raw_data_path
            )
        except Exception as e:
            raise CustomException(e,sys)
        


if __name__ == "__main__":
    obj=Dataingestion_DataProcessing()
    raw_data=obj.intiatedataingestion()
    