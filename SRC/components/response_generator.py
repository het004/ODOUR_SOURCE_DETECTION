import os
from dotenv import load_dotenv
load_dotenv()
import sys
from typing import List, Dict
from dataclasses import dataclass
from SRC.exception import CustomException
from SRC.logger import logging
import requests

# Streamlit import with better handling
try:
    import streamlit as st
    USE_STREAMLIT = True
except (ImportError, ModuleNotFoundError):
    USE_STREAMLIT = False

@dataclass
class ResponseGeneratorConfig:
    output_response_path: str = os.path.join('artifacts', 'generated_response.txt')
    max_response_length: int = 200

class ResponseGenerator:
    def __init__(self):
        self.config = ResponseGeneratorConfig()
        self.api_url = "https://api.mistral.ai/v1/chat/completions"  # Mistral official API
        self.api_key = self._get_api_key()

        if not self.api_key:
            raise ValueError("Mistral API key not found in any available source")

        logging.debug("Mistral API key loaded successfully")

    def _get_api_key(self) -> str:
        key_sources = [
            self._get_key_from_env_vars,
            self._get_key_from_streamlit,
            self._get_key_from_file
        ]
        for source in key_sources:
            try:
                api_key = source()
                if api_key:
                    return api_key
            except Exception as e:
                logging.warning(f"Failed to get API key from {source.__name__}: {str(e)}")
        return None

    def _get_key_from_env_vars(self) -> str:
        return os.getenv("MISTRAL_API_KEY")  # Updated to use Mistral key

    def _get_key_from_streamlit(self) -> str:
        if not USE_STREAMLIT:
            return None
        try:
            return st.secrets.get("MISTRAL_API_KEY")
        except Exception as e:
            logging.warning(f"Streamlit secrets access failed: {str(e)}")
            return None

    def _get_key_from_file(self) -> str:
        try:
            secrets_path = os.path.join(os.path.dirname(__file__), '..', '..', '.streamlit', 'secrets.toml')
            if os.path.exists(secrets_path):
                import toml
                secrets = toml.load(f)
                return secrets.get("secrets", {}).get("MISTRAL_API_KEY")
        except Exception as e:
            logging.warning(f"Failed to read API key from file: {str(e)}")
        return None

    def format_sources(self, sources: List[Dict]) -> str:
        try:
            if not sources:
                return "No odor sources found."
            formatted = []
            for source in sources:
                name = source.get('name', 'Unknown')
                source_type = source.get('type', 'Unknown')
                tags = ", ".join(f"{k}: {v}" for k, v in source.get('tags', {}).items())
                distance = source.get('distance_m', 0)
                formatted.append(
                    f"- Name: {name}, Type: {source_type}, Tags: {tags}, Distance: {distance:.2f} meters"
                )
            return "\n".join(formatted)
        except Exception as e:
            raise CustomException(e, sys)

    def generate_response(self, query: str, location: str, sources: List[Dict]) -> str:
        try:
            formatted_sources = self.format_sources(sources)
            messages = [
                {"role": "system", "content": "You are an assistant helping users identify potential odor sources in Ahmedabad, India. Provide a concise, polite, and natural response summarizing the findings."},
                {"role": "user", "content": f"""Based on the query \"{query}\" and the extracted location \"{location}\", the following potential odor sources were found:\n\n{formatted_sources}\n\nSummarize these findings, including the location and relevant details (e.g., source names, types, distances). If no sources are found, inform the user that no odor sources were identified near the location."""}
            ]

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": "mistral-small-latest",  # FREE-TIER MODEL
                "messages": messages,
                "max_tokens": self.config.max_response_length
            }

            response = requests.post(self.api_url, headers=headers, json=payload)
            response.raise_for_status()

            result = response.json()
            return result["choices"][0]["message"]["content"].strip()

        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed: {str(e)}"
            if hasattr(e, 'response') and e.response:
                error_msg += f" (Status: {e.response.status_code}, Response: {e.response.text[:200]})"
            raise CustomException(error_msg, sys)
        except Exception as e:
            raise CustomException(e, sys)

if __name__ == "__main__":
    from SRC.components.query_processor import QueryProcessor
    processor = QueryProcessor()
    generator = ResponseGenerator()
    query = input("Enter your query (e.g., 'odor in Navrangpura'): ")
    results = processor.process_query(query)
    location = processor.extractor.extract_location(query) or "Unknown"
    response = generator.generate_response(query, location, results)
    print("\nGenerated Response:")
    print(response)
