from langchain_chroma import Chroma
from langchain_community.embeddings import OllamaEmbeddings
import pandas as pd

# Define the list of models to be used for generating embeddings
# modelli = ["gemma2:latest","phi3:medium","phi3.5:latest","mistral-nemo:latest","llama3:latest"]
modelli = ["medllama2","cniongolo/biomistral"]

# Read the CSV file containing the data into a DataFrame
df = pd.read_csv('../data/input/ICD-11_joined.csv')

# Extract the 'prompt' column from the DataFrame as a list of texts
list_text = df["prompt"]

# Loop through each model in the list of models
for modello in modelli:
    # Create a Chroma vector store from the list of texts using the specified model for embeddings
    vectorstore = Chroma.from_texts(
        texts=list_text,  # List of texts to generate embeddings for
        embedding=OllamaEmbeddings(model=modello, show_progress=True),  # Embedding model and settings
        persist_directory=f"../vectorstore/chroma_db-full-{modello.replace(':','')}",  # Directory to persist the vector store
    )
