import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon
import json
from typing import Dict, List

def load_geojson(file_path: str) -> gpd.GeoDataFrame:
    """
    Load a GeoJSON file into a GeoDataFrame.
    
    Args:
        file_path (str): Path to the GeoJSON file
        
    Returns:
        gpd.GeoDataFrame: Loaded GeoDataFrame
    """
    return gpd.read_file(file_path)

def extract_feature_data(row: gpd.GeoSeries) -> Dict:
    """
    Extract relevant data from a GeoJSON feature.
    
    Args:
        row (gpd.GeoSeries): A row from the GeoDataFrame
        
    Returns:
        Dict: Dictionary containing extracted feature data
    """
    # Extract name (use None if not present)
    name = row.get("name", None)
    
    # Determine geometry type
    geom_type = row["geometry"].geom_type
    
    # Extract relevant tags
    tags = {}
    relevant_keys = [
        "amenity", "man_made", "landuse", "industrial",
        "power", "plant:source", "plant:output:electricity", "operator"
    ]
    
    for key in relevant_keys:
        if key in row and pd.notna(row[key]):
            tags[key] = row[key]
    tags_str = json.dumps(tags)
    
    # Calculate centroid for latitude and longitude
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
    """
    Calculate area for polygon features in square meters.
    
    Args:
        row (gpd.GeoSeries): A row from the GeoDataFrame
        gdf (gpd.GeoDataFrame): The complete GeoDataFrame
        idx (int): Index of the current row
        
    Returns:
        float: Area in square meters
    """
    if row["geometry"].geom_type == "Polygon":
        # Reproject to UTM zone 43N for accurate area calculation (Ahmedabad region)
        gdf_utm = gdf.to_crs("EPSG:32643")
        return gdf_utm.loc[idx, "geometry"].area
    return 0

def process_geojson_data(file_path: str, output_path: str) -> pd.DataFrame:
    """
    Process GeoJSON data and save to CSV.
    
    Args:
        file_path (str): Path to input GeoJSON file
        output_path (str): Path to output CSV file
        
    Returns:
        pd.DataFrame: Processed data as a DataFrame
    """
    # Load the GeoJSON file
    gdf = load_geojson(file_path)
    
    # Initialize data structure
    data = {
        "name": [],
        "type": [],
        "tags": [],
        "latitude": [],
        "longitude": [],
        "area_m2": []
    }
    
    # Process each feature
    for idx, row in gdf.iterrows():
        feature_data = extract_feature_data(row)
        if feature_data is None:
            continue
            
        # Add feature data to lists
        for key in ["name", "type", "tags", "latitude", "longitude"]:
            data[key].append(feature_data[key])
            
        # Calculate and add area
        area = calculate_area(row, gdf, idx)
        data["area_m2"].append(area)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    return df

def main():
    """
    Main function to execute the data processing pipeline.
    """
    input_file = "ODOUR_SOURCE_DETECTION\data\export (2).geojson"
    output_file = "ahmedabad_odor_sources_cleaned.csv"
    
    try:
        df = process_geojson_data(input_file, output_file)
        print(f"Data has been cleaned and saved to '{output_file}'")
        print("\nFirst 5 rows of processed data:")
        print(df.head())
    except Exception as e:
        print(f"Error processing data: {str(e)}")

if __name__ == "__main__":
    main()