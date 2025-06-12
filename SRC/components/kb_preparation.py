import os
import sys
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from SRC.exception import CustomException
from SRC.logger import logging
from dataclasses import dataclass
import pickle

@dataclass
class KBPreparationConfig:
    input_csv_path: str = os.path.join('artifacts', 'ahmedabad_odor_sources_cleaned.csv')
    vectorizer_path: str = os.path.join('artifacts', 'tfidf_vectorizer.pkl')
    feature_matrix_path: str = os.path.join('artifacts', 'feature_matrix.pkl')

class KBPreparation:
    def __init__(self):
        self.config = KBPreparationConfig()

    def load_data(self) -> pd.DataFrame:
        """Loads the cleaned CSV file."""
        try:
            logging.info(f"Loading data from {self.config.input_csv_path}")
            df = pd.read_csv(self.config.input_csv_path)
            logging.info("Data loaded successfully.")
            return df
        except Exception as e:
            raise CustomException(e, sys)

    def preprocess_text(self, df: pd.DataFrame) -> pd.Series:
        """Combines relevant text columns into a single string per record for TF-IDF."""
        try:
            logging.info("Preprocessing text data for TF-IDF vectorization.")
            # Assuming 'tags' is a JSON string column
            df['tags_combined'] = df['tags'].apply(lambda x: " ".join(eval(x).values()) if pd.notna(x) else "")
            df['combined_text'] = df[['name', 'type', 'tags_combined']].fillna('').agg(' '.join, axis=1)
            logging.info("Text preprocessing completed.")
            return df['combined_text']
        except Exception as e:
            raise CustomException(e, sys)

    def generate_tfidf_matrix(self, corpus: pd.Series):
        """Generates TF-IDF features and saves the vectorizer and feature matrix."""
        try:
            logging.info("Generating TF-IDF matrix.")
            vectorizer = TfidfVectorizer(max_features=500)
            feature_matrix = vectorizer.fit_transform(corpus)

            # Save vectorizer
            with open(self.config.vectorizer_path, 'wb') as f:
                pickle.dump(vectorizer, f)
            logging.info(f"TF-IDF vectorizer saved at {self.config.vectorizer_path}.")

            # Save feature matrix
            with open(self.config.feature_matrix_path, 'wb') as f:
                pickle.dump(feature_matrix, f)
            logging.info(f"TF-IDF feature matrix saved at {self.config.feature_matrix_path}.")

            return vectorizer, feature_matrix
        except Exception as e:
            raise CustomException(e, sys)

    def initiate_kb_preparation(self):
        """Pipeline: load data -> preprocess -> generate TF-IDF -> save artifacts."""
        try:
            df = self.load_data()
            corpus = self.preprocess_text(df)
            vectorizer, feature_matrix = self.generate_tfidf_matrix(corpus)
            logging.info("Knowledge base preparation completed successfully.")
            return vectorizer, feature_matrix
        except Exception as e:
            raise CustomException(e, sys)

if __name__ == "__main__":
    obj = KBPreparation()
    obj.initiate_kb_preparation()
