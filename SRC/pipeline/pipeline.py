import os
import sys
from dataclasses import dataclass
from SRC.exception import CustomException
from SRC.logger import logging
from SRC.components.data_ingestion_processing import Dataingestion_DataProcessing
from SRC.components.kb_preparation import KBPreparation
from SRC.components.query_processor import QueryProcessor
from SRC.components.response_generator import ResponseGenerator
from SRC.components.query_extractor import QueryExtractor

@dataclass
class PipelineConfig:
    raw_data_path: str = os.path.join('artifacts', 'raw.geojson')
    processed_data_path: str = os.path.join('artifacts', 'ahmedabad_odor_sources_cleaned.csv')
    vectorizer_path: str = os.path.join('artifacts', 'tfidf_vectorizer.pkl')
    feature_matrix_path: str = os.path.join('artifacts', 'feature_matrix.pkl')
    output_response_path: str = os.path.join('artifacts', 'generated_response.txt')

class OdorSourcePipeline:
    def __init__(self):
        self.config = PipelineConfig()
        try:
            self.data_processor = Dataingestion_DataProcessing()
            self.kb_preparer = KBPreparation()
            self.query_processor = QueryProcessor()
            self.response_generator = ResponseGenerator()
            logging.info("OdorSourcePipeline initialized with all components.")
        except Exception as e:
            logging.error(f"Pipeline initialization failed: {str(e)}")
            raise CustomException(e, sys)

    def run_pipeline(self, query: str = None):
        """Execute the full pipeline: data ingestion -> KB preparation -> query processing -> response generation."""
        try:
            logging.info("Starting OdorSourcePipeline execution.")

            # Step 1: Data Ingestion and Processing
            logging.info("Initiating data ingestion and processing.")
            raw_data_path = self.data_processor.intiatedataingestion()
            processed_data_path = self.data_processor.intiatedataProcessor()
            logging.info(f"Data processed and saved to {processed_data_path}")

            # Step 2: Knowledge Base Preparation
            logging.info("Initiating knowledge base preparation.")
            vectorizer, feature_matrix = self.kb_preparer.initiate_kb_preparation()
            logging.info("Knowledge base preparation completed.")

            # Step 3: Query Processing
            if query is None:
                query = input("Enter your query (e.g., 'odor in Vatva'): ")
            logging.info(f"Processing user query: {query}")
            results = self.query_processor.process_query(query)
            location = self.query_processor.extractor.extract_location(query) or "unknown"
            logging.info(f"Query processed, found {len(results)} odor sources near {location}")

            # Step 4: Response Generation
            logging.info("Generating natural language response.")
            response = self.response_generator.generate_response(query, location, results)
            logging.info("Response generated and saved.")

            # Output results for verification
            if results:
                print("\nPotential Odor Sources (sorted by distance):")
                for result in results:
                    print(f"- Name: {result['name']}")
                    print(f"  Type: {result['type']}")
                    print(f"  Tags: {result['tags']}")
                    print(f"  Location: ({result['latitude']}, {result['longitude']})")
                    print(f"  Distance: {result['distance_m']:.2f} meters")
                    print(f"  Distance (Haversine): {result['distance_m_haversine']:.2f} meters")
                    print(f"  Similarity: {result['similarity']:.4f}\n")
            else:
                print("No odor sources found.")
                logging.info("No odor sources found for query.")

            print("\nGenerated Response:")
            print(response)
            return response

        except Exception as e:
            logging.error(f"Pipeline execution failed: {str(e)}")
            raise CustomException(e, sys)

if __name__ == "__main__":
    try:
        pipeline = OdorSourcePipeline()
        pipeline.run_pipeline()
    except Exception as e:
        logging.error(f"Pipeline test execution failed: {str(e)}")
        print(f"Error: {str(e)}")