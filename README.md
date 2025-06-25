
## 🌫️ ODOUR\_SOURCE\_DETECTION

- A full-stack AI system that identifies possible odour pollution sources using map data and natural language queries. This project leverages Overpass Turbo (OpenStreetMap), LLMs with Retrieval-Augmented Generation (RAG), and a user-friendly interface to help engineers and environmental teams trace pollution origin points intelligently.

- It combines geospatial data from OpenStreetMap with Retrieval-Augmented Generation (RAG) techniques to deliver insightful, context-aware answers to queries such as "What might be causing a smell near XYZ location?".

- This project was created for engineers, urban planners, and researchers interested in environmental monitoring, urban air quality analysis, or integrating geospatial AI into smart city applications.

---

### 🧭 Project Objective

To develop an intelligent assistant that detects potential sources of odour in a given location using spatial OpenStreetMap data. It extracts relevant landmarks, facilities, or potential pollutants from the user's query and responds using a domain-specific knowledge base generated dynamically via RAG.
This application:

- Ingests open-source geospatial data (primarily from Overpass Turbo / OpenStreetMap)

- Cleans and formats the data using Python, Pandas, and GeoPandas

- Vectorizes geospatial metadata using TF-IDF for fast semantic search

- Uses an LLM via Retrieval-Augmented Generation (RAG) to generate human-readable answers to location-specific odor-related queries

---

## 🗂️ Project Structure

```
ODOUR_SOURCE_DETECTION/
├── .streamlit/               # Streamlit config
├── artifacts/                # Intermediate outputs and artifacts
├── data/                     # Downloaded and cleaned datasets
├── logs/                     # Log files
├── myenv/                    # Virtual environment
├── SRC/                      # Core backend logic
│   ├── components/           # Functional building blocks
│   │   ├── data_ingestion_processing.py
│   │   ├── kb_preparation.py
│   │   ├── query_extractor.py
│   │   ├── query_processor.py
│   │   └── response_generator.py
│   ├── pipeline/             # Pipeline orchestration
│   │   └── pipeline.py
│   ├── config.py             # Configuration management
│   ├── logger.py             # Logging setup
│   └── exception.py          # Custom exceptions
├── templates/                # Streamlit frontend templates
├── app.py                    # Streamlit app entry point
├── main.py                   # CLI entry point
├── requirements.txt          # Python dependencies
├── setup.py                  # Packaging metadata
├── vercel.json               # Deployment config for Vercel (optional)
├── Railway setup             # ✅ Fully compatible
└── README.md                 # You're here
```

---

## 🔍 Key Features

* 🌍 **OpenStreetMap Integration**: Uses Overpass Turbo queries to retrieve geo-entities from user-defined areas.
* 🧠 **RAG Pipeline**: Builds a knowledge base on-the-fly from ingested data and uses LLMs for response generation.
* 🗨️ **Query Understanding**: Uses NLP to parse and interpret natural language input and relate it to geographic features.
* 🧩 **Modular Architecture**: Clearly separated `components/` and `pipeline/` logic for easy extensibility.
* 🌐 **Streamlit UI**: Lightweight and interactive frontend to run the system locally or remotely.

---
## 🌐 Data Source

The core data source for this project is OpenStreetMap (OSM), accessed through the Overpass Turbo API. The Overpass API allows custom queries to extract features such as:

- Industrial zones

- Waste disposal areas

- Power generation sites

- Landfills and sewage plants

- Chemical processing or pharmaceutical facilities

The data is exported in GeoJSON format and processed with GeoPandas to extract relevant metadata and coordinates, which are then stored in CSV format for efficient use.

## ⚖️ Methodology: Retrieval-Augmented Generation (RAG)

Retrieval-Augmented Generation (RAG) is used to combine structured knowledge retrieval with large language model (LLM) generation.

Steps Involved:

1. Data Ingestion & Cleaning:

- Raw geospatial datasets are imported, cleaned, and normalized.

2. Embedding Creation & Vector Store:

- A corpus is built from cleaned facility descriptions

- TF-IDF vectorization is used to create a searchable knowledge base

3. Query Parsing:

- User input is processed using spaCy NLP pipeline to extract the location

4. Location Geocoding:

- Location names are geocoded to latitude/longitude using Nominatim or similar

5. Context Retrieval:

- Relevant nearby odor-emitting features are retrieved within a defined radius

6. Answer Generation:

- Retrieved context is provided to a local or remote LLM (e.g., Mistral) for response generation

This architecture ensures that the answers are grounded in up-to-date, location-specific knowledge while retaining the natural language generation capabilities of LLMs.

## 🔧 Installation

1. **Clone the repo**

```bash
git clone https://github.com/het004/ODOUR_SOURCE_DETECTION.git
cd ODOUR_SOURCE_DETECTION
```

2. **Create and activate a virtual environment**

```bash
python -m venv myenv
source myenv/bin/activate      # On Windows use `myenv\Scripts\activate`
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```
---
## 🚀 Running the Application

Start the FastAPI Server
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
Access Interactive API Docs

Once the server is running, visit:
```
http://localhost:8000/docs
```
Query the API
```
import requests

query = "What causes the bad smell in Naroda, Ahmedabad?"
response = requests.get("http://localhost:8000/query", params={"text": query})
print(response.json())
```
## 🚀 Running the Project

### Run with Streamlit:

```bash
streamlit run app.py
```

### Run backend processing:

```bash
python main.py
```

> Make sure `.env` includes required API keys and file paths.

---

## 📡 Deployment on Railway (Recommended)

1. Log in to [Railway.app](https://railway.app/)
2. Click **New Project > Deploy from GitHub Repo**
3. Connect your repo and set the following in **Variables** tab:

   ```
   OPENAI_API_KEY=<your_key>
   ```
4. Add `start` command in Railway's service settings:

   ```
   streamlit run app.py --server.port $PORT
   ```
5. Deploy and access the app via Railway's public URL.

📄 You may use `vercel.json` or equivalent if switching to Vercel deployment.

---

## 📁 Component Descriptions

| File / Module                  | Purpose                                          |
| ------------------------------ | ------------------------------------------------ |
| `data_ingestion_processing.py` | Fetch and parse OSM data via Overpass Turbo      |
| `kb_preparation.py`            | Builds retrieval-ready knowledge base            |
| `query_extractor.py`           | Extracts intents and keywords from user queries  |
| `query_processor.py`           | Prepares semantic query embeddings               |
| `response_generator.py`        | Generates LLM-powered responses                  |
| `pipeline.py`                  | Orchestrates full pipeline from input → response |
| `logger.py`, `exception.py`    | Utility for debugging and safe error handling    |

---

## 📊 Example Use Case

```plaintext
User input:
"What's causing the foul odour near Vatva GIDC area?"

System output:
"Nearby sources include: sewage treatment plant (1.2 km), chemical factory (1.6 km),
landfill zone (2.4 km). Based on wind direction and distance, probable source is the STP."
```

---

## 🤝 Contributing

Contributions, issue reports, or feature requests are welcome! Please fork this repo and submit a PR.

---

## 📜 License

MIT License. See `LICENSE` file for details.

---

## 🙋‍♀️ Maintainer

Built with ❤️ by [Het](https://github.com/het004) during the Oizom Internship Program.

---

---

## 📱 Contact

Author: @het004
email: hetshah1718@gmail.com
Issues: Please open issues on GitHub for questions or feature requests.

---

---

🌟 Acknowledgments

OpenStreetMap and Overpass Turbo for open geospatial data

FastAPI for the backend framework

GeoPandas and spaCy for data and NLP processing

Mistral or chosen LLM provider for the RAG pipeline

---
Would you like:

* A badge system (`build passing`, `deployed on Railway`) added?
* A `Dockerfile` or `Procfile` included for deployment flexibility?
* A wiki or documentation site for advanced usage?

Let me know, and I can generate it instantly.
