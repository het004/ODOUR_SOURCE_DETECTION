import os
import sys
from typing import Optional
from geotext import GeoText
import spacy
import re
from SRC.exception import CustomException
from SRC.logger import logging

class QueryExtractor:
    def __init__(self):
        try:
            self.nlp = spacy.load("en_core_web_sm")
            self.ahmedabad_areas = [
                "Navrangpura", "Vastrapur", "Satellite", "Bopal", "Maninagar",
                "Chandkheda", "Thaltej", "Bodakdev", "Ghatlodia", "Isanpur",
                "Vejalpur", "Gota", "Naranpura", "Sabarmati", "Ranip",
                "Ellis Bridge", "Paldi", "Shahibaug", "Memnagar", "Jodhpur",
                "Ambawadi", "Kalupur", "Naroda", "Odhav", "Vatva", "Nikol",
                "Gheekanta", "Jamalpur", "Sola", "Sarkhej", "Hansol", "Asarwa",
                "Dariapur", "Gomtipur", "Behrampura", "Raipur", "Vastral",
                "Amraiwadi", "New Ranip", "Lambha", "Rakhial", "Kubernagar",
                "Chandlodia", "SindhuBhavan"
            ]
            logging.info("QueryExtractor initialized using geotext, SpaCy, and Ahmedabad area list.")
        except Exception as e:
            raise CustomException(e, sys)

    def extract_location(self, user_query: str) -> Optional[str]:
        """
        Extract location from user query:
        Priority 1: GeoText detection
        Priority 2: spaCy NER
        Priority 3: Ahmedabad area name matching with partial matches

        Returns:
            str: Extracted location or None.
        """
        try:
            logging.info(f"Extracting location from user query: {user_query}")

            # Priority 1: Use GeoText to extract cities directly
            places = GeoText(user_query)
            if places.cities:
                location = places.cities[0]  # Return the first city found
                logging.info(f"Location extracted using GeoText: {location}")
                return location

            # Priority 2: spaCy NER (if GeoText fails)
            doc = self.nlp(user_query)
            for ent in doc.ents:
                if ent.label_ == "GPE":
                    logging.info(f"Location extracted using SpaCy NER: {ent.text}")
                    return ent.text

            # Priority 3: Match known Ahmedabad local areas with partial matches
            query_lower = user_query.lower()
            for area in self.ahmedabad_areas:
                # Enhanced regex for partial matches (e.g., "Navrangpura Road")
                if re.search(rf'\b{re.escape(area.lower())}\w*\b', query_lower):
                    logging.info(f"Location matched from Ahmedabad list: {area}")
                    return area

            logging.warning("No location found via GeoText, NER, or Ahmedabad list.")
            return None
        except Exception as e:
            raise CustomException(e, sys)

if __name__ == "__main__":
    extractor = QueryExtractor()
    query = input("Enter your query: ")
    location = extractor.extract_location(query)
    if location:
        print(f"Extracted Location: {location}")
    else:
        print("No location extracted from the query.")