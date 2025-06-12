import os
import sys
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon
import json
from typing import Dict, List
from dataclasses import dataclass

from SRC.exception import CustomException
from SRC.logger import logging

@dataclass
class DataProcessing_config:
    preprocessing_data_path = os.path.join('artifacts', 'ahmedabad_odor_sources_cleaned.csv')
    raw_data_path: str = os.path.join('artifacts', 'raw.geojson')

class Dataingestion_DataProcessing:
    def __init__(self):
        self.ingestion_processing_config = DataProcessing_config()

    def intiatedataingestion(self):
        logging.info("Entered the data ingestion method or component to load a GeoJSON file into a GeoDataFrame.")
        try:
            df = gpd.read_file(r"data\export (2).geojson")
            logging.info("Read the datasets as geopandas GeoDataFrame.")
            os.makedirs(os.path.dirname(self.ingestion_processing_config.raw_data_path), exist_ok=True)
            df.to_file(self.ingestion_processing_config.raw_data_path, index=False)
            logging.info("Ingestion is completed.")
            return self.ingestion_processing_config.raw_data_path
        except Exception as e:
            raise CustomException(e, sys)

    def intiatedataProcessor(self):
        logging.info("Started processing of GeoJSON data.")
        try:
            input_file = self.ingestion_processing_config.raw_data_path
            output_file = self.ingestion_processing_config.preprocessing_data_path

            def load_geojson(file_path: str) -> gpd.GeoDataFrame:
                return gpd.read_file(file_path)

            def extract_feature_data(row: gpd.GeoSeries) -> Dict:
                name = row.get("name", None)
                geom_type = row["geometry"].geom_type
                tags = {}
                relevant_keys = [
                    "amenity", "man_made", "landuse", "industrial",
                    "power", "plant:source", "plant:output:electricity", "operator"
                ]
                for key in relevant_keys:
                    if key in row and pd.notna(row[key]):
                        tags[key] = row[key]
                tags_str = json.dumps(tags)

                if geom_type == "Point":
                    centroid = row["geometry"]
                elif geom_type == "Polygon":
                    centroid = row["geometry"].centroid
                else:
                    return None

                latitude = centroid.y
                longitude = centroid.x

                return {
                    "name": name,
                    "type": geom_type,
                    "tags": tags_str,
                    "latitude": latitude,
                    "longitude": longitude
                }

            def calculate_area(row: gpd.GeoSeries, gdf: gpd.GeoDataFrame, idx: int) -> float:
                if row["geometry"].geom_type == "Polygon":
                    gdf_utm = gdf.to_crs("EPSG:32643")
                    return gdf_utm.loc[idx, "geometry"].area
                return 0

            def process_geojson_data(file_path: str, output_path: str) -> pd.DataFrame:
                gdf = load_geojson(file_path)
                data = {
                    "name": [],
                    "type": [],
                    "tags": [],
                    "latitude": [],
                    "longitude": [],
                    "area_m2": []
                }
                for idx, row in gdf.iterrows():
                    feature_data = extract_feature_data(row)
                    if feature_data is None:
                        continue
                    for key in ["name", "type", "tags", "latitude", "longitude"]:
                        data[key].append(feature_data[key])
                    area = calculate_area(row, gdf, idx)
                    data["area_m2"].append(area)

                df = pd.DataFrame(data)
                df.to_csv(output_path, index=False)
                return df

            df = process_geojson_data(input_file, output_file)
            logging.info(f"Data has been processed and saved to {output_file}")
            logging.info(f"First 5 rows of processed data:\n{df.head()}")
            return output_file

        except Exception as e:
            raise CustomException(e, sys)

if __name__ == "__main__":
    obj = Dataingestion_DataProcessing()
    raw_data = obj.intiatedataingestion()
    processed_data = obj.intiatedataProcessor()
