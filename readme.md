# Project Overview

This project is focused on processing medical case data using Large Language Models and LangChain for retrieval-augmented generation (RAG). The system extracts, processes, and diagnoses clinical cases using ICD-11 codes.

## Features

- Extracts and processes clinical case data from CSV and PDF files.
- Uses LangChain with Chroma vector stores for information retrieval.
- Queries large language models (LLMs) such as `medllama2` and `biomistral`.
- Retrieves ICD-11 classification codes via API.
- Generates diagnostic answers for medical cases using RAG.

## Setup Instructions

### Prerequisites

Ensure you have Python 3.8+ installed along with the necessary dependencies. Install them using:

```bash
pip install -r requirements.txt
```

### Running the Project

1. **Download ICD-11 Data**

   ```bash
   python icddownloader.py
   ```

   This script fetches ICD-11 classifications from an API and stores the data in JSON and CSV format.

2. **Process ICD-11 Data**

   ```bash
   python ICD_processing.py
   ```

   This script processes and cleans ICD-11 data, preparing it for use in the pipeline.

3. **Split DSM-5 Cases**

   ```bash
   python dsmsplit.py
   ```

   Extracts and structures DSM-5 case data from a text file into a structured CSV.

4. **Generate Vector Embeddings**

   ```bash
   python langchainbuilder.py
   ```

   Builds Chroma vector stores for efficient retrieval.

5. **Process Medical Cases with AI**

   ```bash
   python app.py
   ```

   Runs the main pipeline to generate diagnoses for clinical cases.

6. **Expose LLM-based API**

   ```bash
   python api.py
   ```

   Provides a Flask API endpoint (`/askLLM`) that allows querying an LLM.

## File Descriptions

### `api.py`

- Implements a Flask-based REST API for querying an LLM using LangChain.
- Retrieves data using Chroma and LangChain's RAG capabilities.
- Accepts a JSON request with an `input_string` and returns the LLM response.

### `app.py`

- Reads DSM-5 cases from CSV.
- Uses Chroma and LLMs to generate diagnostic outputs.
- Saves results in a structured format.

### `dsmsplit.py`

- Extracts individual DSM-5 cases from a text file.
- Parses cases into structured sections: `Introduction`, `Discussion`, `Diagnosis`.
- Saves the structured data into a CSV file.

### `ICD_processing.py`

- Reads ICD-11 classifications from a CSV.
- Extracts ICD-11 disorder descriptions from a PDF.
- Cleans and structures data for use in LangChain-based queries.

### `icddownloader.py`

- Fetches ICD-11 classification data from an API.
- Saves data in JSON and CSV formats for further processing.

### `langchainbuilder.py`

- Creates vector embeddings using LangChain’s Chroma module.
- Loads ICD-11 prompts for efficient retrieval-augmented generation (RAG).

## API Usage

To query the API, send a POST request to `http://127.0.0.1:5000/askLLM` with JSON data:

```json
{
  "input_string": "Describe the symptoms of schizophrenia."
}
```

The response will contain the AI-generated diagnosis:

```json
{
  "output_string": "Schizophrenia is characterized by..."
}
```

## Acknowledgments

This project leverages LangChain, Chroma, and various open medical datasets to improve automated clinical case processing and diagnostics.

