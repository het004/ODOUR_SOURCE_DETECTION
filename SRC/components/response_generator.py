import os
import sys
from typing import List, Dict
from dataclasses import dataclass
from langchain_community.llms import Ollama
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from SRC.exception import CustomException
from SRC.logger import logging
from dotenv import load_dotenv

# Load environment variables for LangChain tracing
load_dotenv()
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGCHAIN_API_KEY")

@dataclass
class ResponseGeneratorConfig:
    output_response_path: str = os.path.join('artifacts', 'generated_response.txt')
    model_name: str = "mistral"  # Ollama model name
    max_response_length: int = 200  # Max tokens for generated response

class ResponseGenerator:
    def __init__(self):
        self.config = ResponseGeneratorConfig()
        try:
            logging.info(f"Initializing ResponseGenerator with Ollama model {self.config.model_name}")
            # Verify LangChain tracing setup
            if not os.getenv("LANGCHAIN_API_KEY"):
                logging.warning("LANGCHAIN_API_KEY not set. Tracing will not work.")
            # Initialize Ollama with Mistral model
            self.llm = Ollama(
                model=self.config.model_name,
                base_url="http://localhost:11434",  # Default Ollama endpoint
                num_predict=self.config.max_response_length,
                temperature=0.7,
                top_p=0.9
            )
            # Define chat prompt template
            self.prompt_template = ChatPromptTemplate.from_messages([
                ("system", "You are an assistant helping users identify potential odor sources in Ahmedabad, India. Provide a concise, polite, and natural response summarizing the findings."),
                ("user", """Based on the query "{query}" and the extracted location "{location}", the following potential odor sources were found:

{sources}

Summarize these findings, including the location and relevant details (e.g., source names, types, distances). If no sources are found, inform the user that no odor sources were identified near the location.""")
            ])
            # Set up LangChain chain
            self.chain = self.prompt_template | self.llm | StrOutputParser()
            logging.info("ResponseGenerator initialized successfully.")
        except Exception as e:
            raise CustomException(e, sys)

    def format_sources(self, sources: List[Dict]) -> str:
        """Format odor sources into a string for the prompt."""
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
        """Generate a natural language response using Ollama's Mistral model and LangChain."""
        try:
            logging.info(f"Generating response for query: {query}, location: {location}")
            formatted_sources = self.format_sources(sources)
            # Invoke LangChain chain (traced to LangSmith if enabled)
            response = self.chain.invoke({
                "query": query,
                "location": location,
                "sources": formatted_sources
            })
            # Trim response to remove unnecessary tokens
            response = response.strip()
            # Save response to file
            os.makedirs(os.path.dirname(self.config.output_response_path), exist_ok=True)
            with open(self.config.output_response_path, 'w') as f:
                f.write(response)
            logging.info(f"Response saved to {self.config.output_response_path}")
            return response
        except Exception as e:
            raise CustomException(e, sys)

if __name__ == "__main__":
    from query_processor import QueryProcessor
    # Test the response generator
    processor = QueryProcessor()
    generator = ResponseGenerator()
    query = input("Enter your query (e.g., 'odor in Navrangpura'): ")
    results = processor.process_query(query)
    location = processor.extractor.extract_location(query) or "Unknown"
    response = generator.generate_response(query, location, results)
    print("\nGenerated Response:")
    print(response)