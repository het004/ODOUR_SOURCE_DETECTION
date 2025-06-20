import os
import sys
import pandas as pd
from fastapi import FastAPI, Form, Request, Depends
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from io import StringIO
from SRC.pipeline.pipeline import OdorSourcePipeline
from SRC.exception import CustomException

# Add SRC to sys.path to ensure imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'SRC')))

# Initialize FastAPI app
app = FastAPI(
    title="Odor Source Detection - Ahmedabad",
    description="A FastAPI application to identify potential odor sources in Ahmedabad, India.",
    version="1.0.0"
)

# Initialize Jinja2 templates
templates = Jinja2Templates(directory="templates")

# Initialize the pipeline
try:
    pipeline = OdorSourcePipeline()
except Exception as e:
    print(f"Failed to initialize pipeline: {str(e)}")
    raise

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Render the home page with the query input form."""
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "results": None, "summary": None, "location": None, "error": None}
    )

@app.post("/find_odor_sources", response_class=HTMLResponse)
async def find_odor_sources(request: Request, query: str = Form(...)):
    """Handle query submission, process it, and return results."""
    if not query:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "results": None,
                "summary": None,
                "location": None,
                "error": "Please enter a query to proceed."
            }
        )

    try:
        # Run the pipeline with the user's query
        response = pipeline.run_pipeline(query=query)
        results = pipeline.query_processor.process_query(query)
        location = pipeline.query_processor.extractor.extract_location(query) or "unknown"

        # Prepare data for table display
        data = []
        if results:
            if isinstance(results, (list, tuple)):
                for result in results:
                    data.append({
                        "Name": result.get('name', 'Unknown'),
                        "Type": result.get('type', 'Unknown'),
                        "Tags": ", ".join(f"{k}: {v}" for k, v in result.get('tags', {}).items()),
                        "Location": f"({result.get('latitude', 0)}, {result.get('longitude', 0)})",
                        "Distance (UTM)": f"{result.get('distance_m', 0):.2f} meters",
                        "Distance (Haversine)": f"{result.get('distance_m_haversine', 0):.2f} meters",
                        "Similarity": f"{result.get('similarity', 0):.4f}"
                    })
            else:
                print(f"Unexpected results type: {type(results)} - {results}")

        # Render the results page
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "results": data,
                "summary": response,
                "location": location,
                "error": None
            }
        )

    except CustomException as e:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "results": None,
                "summary": None,
                "location": None,
                "error": f"An error occurred: {str(e)}. Please check the logs or ensure all dependencies are met."
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "results": None,
                "summary": None,
                "location": None,
                "error": f"An unexpected error occurred: {str(e)}. Please try again."
            }
        )

@app.post("/download_csv")
async def download_csv(query: str = Form(...)):
    """Generate and return a CSV file of the results."""
    try:
        results = pipeline.query_processor.process_query(query)
        if results:
            data = [
                {
                    "Name": result.get('name', 'Unknown'),
                    "Type": result.get('type', 'Unknown'),
                    "Tags": ", ".join(f"{k}: {v}" for k, v in result.get('tags', {}).items()),
                    "Location": f"({result.get('latitude', 0)}, {result.get('longitude', 0)})",
                    "Distance (UTM)": f"{result.get('distance_m', 0):.2f} meters",
                    "Distance (Haversine)": f"{result.get('distance_m_haversine', 0):.2f} meters",
                    "Similarity": f"{result.get('similarity', 0):.4f}"
                }
                for result in results
            ]
            df = pd.DataFrame(data)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            return StreamingResponse(
                csv_buffer,
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=odor_sources.csv"}
            )
        else:
            return {"error": "No results to download."}
    except Exception as e:
        return {"error": f"Failed to generate CSV: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
