import os
import sys
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from geopy.geocoders import Nominatim
import pickle
from sklearn.metrics.pairwise import cosine_similarity
import json
from math import radians, sin, cos, sqrt, atan2
from typing import Optional, List, Dict
from dataclasses import dataclass
from SRC.exception import CustomException
from SRC.logger import logging
from SRC.components.query_extractor import QueryExtractor
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
            logging.error(f"QueryProcessor initialization failed: {str(e)}")
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
            logging.error(f"Geocoding failed for location {location}: {str(e)}")
            raise CustomException(e, sys)

    def haversine(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate the great-circle distance between two points in meters."""
        R = 6371000  # Earth radius in meters
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        return R * c

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
            nearby_sources = gdf_utm[gdf_utm.geometry.intersects(buffer.iloc[0])].copy()
            # Calculate distances in meters (UTM-based)
            if not nearby_sources.empty:
                nearby_sources.loc[:, 'distance_m'] = nearby_sources.geometry.apply(
                    lambda geom: query_point_utm.distance(geom)
                )
                # Calculate Haversine distances for comparison
                nearby_sources.loc[:, 'distance_m_haversine'] = nearby_sources.apply(
                    lambda row: self.haversine(latitude, longitude, row['latitude'], row['longitude']),
                    axis=1
                )
                logging.info(f"Found {len(nearby_sources)} odor sources within {self.config.search_radius_m}m")
            else:
                logging.info("No odor sources found within search radius")
            # Convert back to lat/lon for output
            nearby_sources = nearby_sources.to_crs("EPSG:4326")
            return nearby_sources
        except Exception as e:
            logging.error(f"Finding nearby sources failed: {str(e)}")
            raise CustomException(e, sys)

    def filter_odor_sources(self, gdf: gpd.GeoDataFrame, query: str) -> gpd.GeoDataFrame:
        """Filter sources based on odor-related keywords and query similarity."""
        try:
            logging.info("Filtering odor sources based on tags and query similarity")
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
            odor_sources = gdf[gdf['tags'].apply(is_odor_source)].copy()

            # Further refine using TF-IDF similarity
            if not query or odor_sources.empty:
                logging.info(f"No query or no odor sources to filter, returning {len(odor_sources)} sources")
                return odor_sources

            # Transform query to TF-IDF vector
            logging.debug("Computing TF-IDF similarity for query")
            query_vector = self.vectorizer.transform([query])
            similarities = cosine_similarity(query_vector, self.feature_matrix).flatten()
            # Add similarity scores to DataFrame
            if not odor_sources.empty:
                odor_sources.loc[:, 'similarity'] = similarities[odor_sources.index]
                logging.info(f"Filtered {len(odor_sources)} odor sources with similarity scores")
            else:
                logging.info("No odor sources after filtering")
            return odor_sources
        except Exception as e:
            logging.error(f"Odor source filtering failed: {str(e)}")
            raise CustomException(e, sys)

    def process_query(self, query: str) -> List[Dict]:
        """Main method to process a user query and return nearby odor sources in ascending order of distance."""
        try:
            logging.info(f"Processing query: {query}")
            # Step 1: Extract location
            location = self.extractor.extract_location(query)
            if not location:
                logging.warning("No location extracted from query")
                return []

            # Step 2: Geocode location
            coords = self.geocode_location(location)
            if not coords:
                logging.warning(f"Could not geocode location: {location}")
                return []

            # Step 3: Find nearby sources
            nearby_sources = self.find_nearby_sources(coords['latitude'], coords['longitude'])
            if nearby_sources.empty:
                logging.info("No odor sources found within search radius")
                return []

            # Step 4: Filter odor-relevant sources
            odor_sources = self.filter_odor_sources(nearby_sources, query)
            if odor_sources.empty:
                logging.info("No odor-relevant sources found after filtering")
                return []

            # Step 5: Reset index and sort by distance_m in ascending order
            odor_sources = odor_sources.reset_index(drop=True)
            odor_sources = odor_sources.sort_values(by='distance_m', ascending=True)
            logging.info("Sorted odor sources by distance_m in ascending order")

            # Step 6: Format output
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
                    "distance_m_haversine": row['distance_m_haversine'],
                    "similarity": row.get('similarity', 0.0)
                })
            logging.info(f"Processed query, found {len(results)} odor sources")
            return results
        except Exception as e:
            logging.error(f"Query processing failed: {str(e)}")
            raise CustomException(e, sys)

if __name__ == "__main__":
    try:
        logging.info("Starting QueryProcessor test via console interface")
        processor = QueryProcessor()
        query = input("Enter your query (e.g., 'odor in Navrangpura'): ")
        results = processor.process_query(query)
        if results:
            logging.info("Displaying query results")
            print("\nPotential Odor Sources (sorted by distance):")
            for result in results:
                print(f"- Name: {result['name']}")
                print(f"  Type: {result['type']}")
                print(f"  Tags: {result['tags']}")
                print(f"  Location: ({result['latitude']}, {result['longitude']})")
                print(f"  Distance (UTM): {result['distance_m']:.2f} meters")
                print(f"  Distance (Haversine): {result['distance_m_haversine']:.2f} meters")
                print(f"  Similarity: {result['similarity']:.4f}\n")
        else:
            print("No odor sources found.")
            logging.info("No odor sources found for query")
    except Exception as e:
        logging.error(f"Test execution failed: {str(e)}")
        print(f"Error: {str(e)}")