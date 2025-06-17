import os
import sys
import streamlit as st
import pandas as pd

# Add SRC to sys.path to ensure imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'SRC')))

from SRC.pipeline.pipeline import OdorSourcePipeline
from SRC.exception import CustomException

# Streamlit page configuration
st.set_page_config(
    page_title="Odor Source Detection - Ahmedabad",
    page_icon="🌬️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and description
st.title("🌬️ Odor Source Detection in Ahmedabad")
st.markdown("""
This app helps you identify potential odor sources in Ahmedabad, India, based on your query.
Enter a location-based query (e.g., "odor in Vatva") to find nearby odor sources within a 5 km radius,
sorted by distance, along with a natural language summary of the findings.
""")

# Sidebar for instructions
with st.sidebar:
    st.header("How to Use")
    st.markdown("""
    1. Enter a query in the format "odor in [location]" (e.g., "odor in Vatva").
    2. Click the "Find Odor Sources" button to process your query.
    3. View the results, including a list of potential odor sources and a summary.
    """)
    st.info("Ensure the Ollama server is running with the `tinyllama` model on `http://localhost:11434`.")

# Input query
query = st.text_input("Enter your query:", placeholder="e.g., odor in Vatva", key="query_input")

# Button to run the pipeline
if st.button("Find Odor Sources"):
    if not query:
        st.warning("Please enter a query to proceed.")
    else:
        with st.spinner("Processing your query..."):
            try:
                # Initialize the pipeline
                pipeline = OdorSourcePipeline()
                
                # Run the pipeline with the user's query
                response = pipeline.run_pipeline(query=query)

                # Retrieve results and location from the pipeline instance
                results = pipeline.query_processor.process_query(query)
                location = pipeline.query_processor.extractor.extract_location(query) or "unknown"

                # Display results
                if results:
                    st.success(f"Found {len(results)} potential odor sources near {location}.")
                    
                    # Prepare data for table display
                    data = []
                    for result in results:
                        data.append({
                            "Name": result['name'],
                            "Type": result['type'],
                            "Tags": ", ".join(f"{k}: {v}" for k, v in result['tags'].items()),
                            "Location": f"({result['latitude']}, {result['longitude']})",
                            "Distance (UTM)": f"{result['distance_m']:.2f} meters",
                            "Distance (Haversine)": f"{result['distance_m_haversine']:.2f} meters",
                            "Similarity": f"{result['similarity']:.4f}"
                        })
                    
                    # Display results in a table
                    st.subheader("Potential Odor Sources (Sorted by Distance)")
                    st.table(pd.DataFrame(data))
                    
                    # Display the generated response
                    st.subheader("Summary")
                    st.write(response)
                    
                    # Option to download results as CSV
                    df = pd.DataFrame(data)
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="Download Results as CSV",
                        data=csv,
                        file_name="odor_sources.csv",
                        mime="text/csv"
                    )
                else:
                    st.info(f"No odor sources found near {location}.")
                    st.subheader("Summary")
                    st.write(response)
                    
            except CustomException as e:
                st.error(f"An error occurred: {str(e)}")
                st.markdown("Please check the logs for more details or ensure all dependencies are met (e.g., GeoJSON file, Ollama server).")
            except Exception as e:
                st.error(f"An unexpected error occurred: {str(e)}")
                st.markdown("Please try again or contact support.")

# Footer
st.markdown("---")
st.markdown("Developed by [Het Shah] | Powered by Streamlit | June 2025")