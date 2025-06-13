import os
import sys
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from geopy.geocoders import Nominatim
import pickle
from sklearn.metrics.pairwise import cosine_similarity
import json
from typing import Optional, List, Dict
from dataclasses import dataclass
from SRC.exception import CustomException
from SRC.logger import logging
from query_extractor import QueryExtractor

@dataclass
class QueryProcessorConfig:
    input_csv_path: str = os.path.join('artifacts', 'ahmedabad_odor_sources_cleaned.csv')
    vectorizer_path: str = os.path.join('artifacts', 'tfidf_vectorizer.pkl')
    feature_matrix_path: str = os.path.join('artifacts', 'feature_matrix.pkl')
    search_radius_m: float = 5000.0  # Search radius in meters (5 km)

class QueryProcessor:
    def __init__(self):
        self.config = QueryProcessorConfig()
        self.extractor = QueryExtractor()
        self.geolocator = Nominatim(user_agent="odor_source_finder")
        try:
            # Load knowledge base artifacts
            self.df = pd.read_csv(self.config.input_csv_path)
            with open(self.config.vectorizer_path, 'rb') as f:
                self.vectorizer = pickle.load(f)
            with open(self.config.feature_matrix_path, 'rb') as f:
                self.feature_matrix = pickle.load(f)
            logging.info("QueryProcessor initialized with knowledge base artifacts.")
        except Exception as e:
            raise CustomException(e, sys)

    def geocode_location(self, location: str) -> Optional[Dict[str, float]]:
        """Convert a location name to GPS coordinates using Nominatim."""
        try:
            logging.info(f"Geocoding location: {location}")
            geocoded = self.geolocator.geocode(f"{location}, Ahmedabad, India", timeout=10)
            if geocoded:
                logging.info(f"Geocoded {location} to ({geocoded.latitude}, {geocoded.longitude})")
                return {"latitude": geocoded.latitude, "longitude": geocoded.longitude}
            logging.warning(f"No coordinates found for location: {location}")
            return None
        except Exception as e:
            raise CustomException(e, sys)

    def find_nearby_sources(self, latitude: float, longitude: float) -> gpd.GeoDataFrame:
        """Find odor sources within the search radius of the given coordinates."""
        try:
            logging.info(f"Finding odor sources near ({latitude}, {longitude})")
            # Create GeoDataFrame from CSV
            gdf = gpd.GeoDataFrame(
                self.df,
                geometry=gpd.points_from_xy(self.df.longitude, self.df.latitude),
                crs="EPSG:4326"
            )
            # Convert to UTM for accurate distance calculations
            gdf_utm = gdf.to_crs("EPSG:32643")
            query_point = Point(longitude, latitude)
            query_point_utm = gpd.GeoSeries([query_point], crs="EPSG:4326").to_crs("EPSG:32643")
            # Create buffer (search radius in meters)
            buffer = query_point_utm.buffer(self.config.search_radius_m)
            # Find sources within buffer
            nearby_sources = gdf_utm[gdf_utm.geometry.intersects(buffer.iloc[0])]
            # Convert back to lat/lon for output
            nearby_sources = nearby_sources.to_crs("EPSG:4326")
            # Calculate distances in meters
            nearby_sources['distance_m'] = nearby_sources.geometry.apply(
                lambda geom: query_point_utm.distance(gpd.GeoSeries([geom], crs="EPSG:32643")).iloc[0]
            )
            return nearby_sources
        except Exception as e:
            raise CustomException(e, sys)

    def filter_odor_sources(self, gdf: gpd.GeoDataFrame, query: str) -> gpd.GeoDataFrame:
        """Filter sources based on odor-related keywords and query similarity."""
        try:
            logging.info("Filtering odor sources based on tags and query similarity.")
            # Odor-related tags (e.g., landfill, waste, industrial)
            odor_keywords = [
                "landfill", "waste", "industrial", "sewage", "dump", "garbage",
                "chemical", "factory", "slaughterhouse", "refinery"
            ]
            def is_odor_source(tags_str: str) -> bool:
                try:
                    tags = json.loads(tags_str) if pd.notna(tags_str) else {}
                    return any(keyword in str(tags.values()).lower() for keyword in odor_keywords)
                except json.JSONDecodeError:
                    return False

            # Filter sources with odor-related tags
            odor_sources = gdf[gdf['tags'].apply(is_odor_source)]

            # Further refine using TF-IDF similarity
            if not query:
                return odor_sources

            # Transform query to TF-IDF vector
            query_vector = self.vectorizer.transform([query])
            similarities = cosine_similarity(query_vector, self.feature_matrix).flatten()
            # Add similarity scores to DataFrame
            odor_sources['similarity'] = similarities[odor_sources.index]
            # Sort by similarity and distance
            odor_sources = odor_sources.sort_values(by=['similarity', 'distance_m'], ascending=[False, True])
            return odor_sources
        except Exception as e:
            raise CustomException(e, sys)

    def process_query(self, query: str) -> List[Dict]:
        """Main method to process a user query and return nearby odor sources."""
        try:
            logging.info(f"Processing query: {query}")
            # Step 1: Extract location
            location = self.extractor.extract_location(query)
            if not location:
                logging.warning("No location extracted from query.")
                return []

            # Step 2: Geocode location
            coords = self.geocode_location(location)
            if not coords:
                logging.warning(f"Could not geocode location: {location}")
                return []

            # Step 3: Find nearby sources
            nearby_sources = self.find_nearby_sources(coords['latitude'], coords['longitude'])
            if nearby_sources.empty:
                logging.info("No odor sources found within search radius.")
                return []

            # Step 4: Filter odor-relevant sources
            odor_sources = self.filter_odor_sources(nearby_sources, query)
            if odor_sources.empty:
                logging.info("No odor-relevant sources found after filtering.")
                return []

            # Step 5: Format output
            results = []
            for _, row in odor_sources.iterrows():
                tags = json.loads(row['tags']) if pd.notna(row['tags']) else {}
                results.append({
                    "name": row['name'] if pd.notna(row['name']) else "Unknown",
                    "type": row['type'],
                    "tags": tags,
                    "latitude": row['latitude'],
                    "longitude": row['longitude'],
                    "distance_m": row['distance_m'],
                    "similarity": row.get('similarity', 0.0)
                })

            logging.info(f"Found {len(results)} odor sources for query: {query}")
            return results
        except Exception as e:
            raise CustomException(e, sys)

if __name__ == "__main__":
    processor = QueryProcessor()
    query = input("Enter your query (e.g., 'odor in Navrangpura'): ")
    results = processor.process_query(query)
    if results:
        print("\nPotential Odor Sources:")
        for result in results:
            print(f"- Name: {result['name']}")
            print(f"  Type: {result['type']}")
            print(f"  Tags: {result['tags']}")
            print(f"  Location: ({result['latitude']}, {result['longitude']})")
            print(f"  Distance: {result['distance_m']:.2f} meters")
            print(f"  Similarity: {result['similarity']:.4f}\n")
    else:
        print("No odor sources found for the query.")