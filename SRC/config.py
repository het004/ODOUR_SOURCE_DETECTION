# src/config.py

# Default search radius in meters
SEARCH_RADIUS = 2000  # 2 km

# Default city to geocode with location name
DEFAULT_CITY = "Ahmedabad"

# OpenStreetMap Nominatim geocoding API (for getting lat/lon from location names)
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Overpass API endpoint (for fetching OSM data)
OVERPASS_URL = "https://overpass-turbo.eu/s/25Ne"

# User-Agent header (some APIs require this to prevent blocking)
HEADERS = {
    "User-Agent": "OdourDetectionApp/1.0 (contact@example.com)"
}
