import os
from dotenv import load_dotenv
load_dotenv()  # Load .env for local testing
import sys
from typing import List, Dict
from dataclasses import dataclass
from SRC.exception import CustomException
from SRC.logger import logging
import requests

try:
    import streamlit as st
    USE_STREAMLIT = True
except ImportError:
    USE_STREAMLIT = False

@dataclass
class ResponseGeneratorConfig:
    output_response_path: str = os.path.join('artifacts', 'generated_response.txt')
    max_response_length: int = 200

class ResponseGenerator:
    def __init__(self):
        self.config = ResponseGeneratorConfig()
        self.api_url = "https://router.huggingface.co/nebius/v1/chat/completions"
        if USE_STREAMLIT:
            self.api_key = st.secrets["HF_API_KEY"]
        else:
            self.api_key = os.getenv("HF_API_KEY")  # Local environment

        print("Loaded HF_API_KEY:", self.api_key)  # Debug print, remove in production

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

            headers = {"Authorization": f"Bearer {self.api_key}"}
            payload = {
                "messages": messages,
                "model": "deepseek-ai/DeepSeek-R1-fast"
            }

            response = requests.post(self.api_url, headers=headers, json=payload)
            if response.status_code == 200:
                result = response.json()
                return result["choices"][0]["message"]["content"].strip()
            else:
                raise Exception(f"HuggingFace API error: {response.status_code} - {response.text}")
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
