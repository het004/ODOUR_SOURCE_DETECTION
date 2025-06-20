import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from SRC.exception import CustomException
from SRC.logger import logging
from SRC.components.data_ingestion_processing import Dataingestion_DataProcessing
from SRC.components.kb_preparation import KBPreparation
from SRC.components.query_processor import QueryProcessor
from SRC.components.response_generator import ResponseGenerator

@dataclass
class PipelineConfig:
    """Configuration for pipeline paths and parameters"""
    artifacts_dir: str = os.path.join('artifacts')
    data_dir: str = os.path.join('data')
    raw_data_path: str = os.path.join('artifacts', 'raw.geojson')
    processed_data_path: str = os.path.join('artifacts', 'ahmedabad_odor_sources_cleaned.csv')
    vectorizer_path: str = os.path.join('artifacts', 'tfidf_vectorizer.pkl')
    feature_matrix_path: str = os.path.join('artifacts', 'feature_matrix.pkl')
    output_response_path: str = os.path.join('artifacts', 'generated_response.txt')
    geojson_filename: str = 'export.geojson'

class OdorSourcePipeline:
    def __init__(self):
        self.config = PipelineConfig()
        try:
            self._verify_environment()
            self._initialize_components()
            logging.info("OdorSourcePipeline successfully initialized")
        except Exception as e:
            self._log_environment_details()
            raise CustomException(e, sys)

    def _verify_environment(self):
        """Verify all required directories and files exist"""
        Path(self.config.artifacts_dir).mkdir(parents=True, exist_ok=True)
        Path(self.config.data_dir).mkdir(parents=True, exist_ok=True)

        data_file = os.path.join(self.config.data_dir, self.config.geojson_filename)
        if not os.path.exists(data_file):
            raise FileNotFoundError(
                f"Required data file not found at {data_file}. "
                f"Please ensure the file exists in the data directory."
            )

    def _initialize_components(self):
        """Initialize all pipeline components"""
        self.data_processor = Dataingestion_DataProcessing()
        self.kb_preparer = KBPreparation()
        self.query_processor = QueryProcessor()
        self.response_generator = ResponseGenerator()

    def _log_environment_details(self):
        """Log environment details for debugging"""
        logging.error(f"Current directory: {os.getcwd()}")
        logging.error(f"Directory contents: {os.listdir('.')}")
        if os.path.exists(self.config.data_dir):
            logging.error(f"Data directory contents: {os.listdir(self.config.data_dir)}")

    def run_pipeline(self, query: Optional[str] = None) -> str:
        """Execute the full pipeline"""
        try:
            logging.info("Starting pipeline execution")
            
            # Step 1: Data Processing - CORRECTED METHOD NAME HERE
            raw_data_path = self.data_processor.intiatedataingestion()  # Fixed method name
            self._verify_file(raw_data_path, "Raw data file")
            
            processed_data_path = self.data_processor.intiatedataProcessor()  # Fixed method name
            self._verify_file(processed_data_path, "Processed data file")
            logging.info(f"Data processing completed. Output at {processed_data_path}")

            # Step 2: Knowledge Base Preparation
            self.kb_preparer.initiate_kb_preparation()
            self._verify_file(self.config.vectorizer_path, "Vectorizer file")
            self._verify_file(self.config.feature_matrix_path, "Feature matrix file")
            logging.info("Knowledge base preparation completed")

            # Step 3: Get user query
            if query is None:
                query = input("Enter your query (e.g., 'odor in Vatva'): ")

            # Step 4: Process query and generate response
            logging.info(f"Processing query: {query}")
            results = self.query_processor.process_query(query)
            location = self.query_processor.extractor.extract_location(query) or "unknown location"
            
            if not results:
                logging.info(f"No odor sources found near {location}")
                return f"No odor sources were identified near {location}."
            
            logging.info(f"Found {len(results)} odor sources near {location}")
            response = self.response_generator.generate_response(query, location, results)
            
            self._log_query_results(results, location)
            return response
            
        except Exception as e:
            logging.error(f"Pipeline execution failed: {str(e)}")
            raise CustomException(e, sys)

    def _verify_file(self, filepath: str, description: str) -> None:
        """Verify a file exists"""
        if not os.path.exists(filepath):
            dir_contents = os.listdir(os.path.dirname(filepath)) if os.path.exists(os.path.dirname(filepath)) else []
            raise FileNotFoundError(
                f"{description} not found at {filepath}. "
                f"Directory contents: {dir_contents}"
            )

    def _log_query_results(self, results: list, location: str) -> None:
        """Log query results"""
        logging.info(f"Query Results for {location}:")
        for i, result in enumerate(results[:5]):
            logging.info(
                f"Result {i+1}: {result['name']} | "
                f"Distance: {result['distance_m']:.2f}m | "
                f"Type: {result['type']}"
            )

if __name__ == "__main__":
    try:
        pipeline = OdorSourcePipeline()
        response = pipeline.run_pipeline()
        print("\nPipeline Results:")
        print(response)
    except Exception as e:
        print(f"\nError: {str(e)}")
        print("Troubleshooting Tips:")
        print("- Check the spelling of method names in your components")
        print("- Verify all required files exist in the data directory")
        print("- Review the logs for more detailed error information")
        sys.exit(1)