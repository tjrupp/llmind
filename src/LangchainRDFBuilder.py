import pandas as pd
from langchain_community.embeddings import OllamaEmbeddings
from langchain_chroma import Chroma
from typing import List, Optional
import subprocess  # For running Ollama commands
import threading
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF, RDFS, OWL  # Import RDF, RDFS, and OWL namespaces


def get_available_ollama_models() -> List[str]:
    """
    Retrieves a list of available Ollama models.

    Returns:
        A list of model names, or an empty list on error.
    """
    try:
        # Run the `ollama list` command and capture the output
        result = subprocess.run(['ollama', 'list'], capture_output=True, text=True, check=True)
        output = result.stdout

        # Parse the output.  Ollama list outputs a table, which we need to convert to a list of names
        lines = output.strip().split('\n')[1:]  # Skip the header line
        models = []
        for line in lines:
            parts = line.split()
            if len(parts) > 1:
                models.append(parts[0])  # The first part of each line is the model name
        return models
    except subprocess.CalledProcessError as e:
        print(f"Error listing Ollama models: {e}")
        return []  # Return an empty list in case of an error
    except Exception as e:
        print(f"An unexpected error occurred while listing Ollama models: {e}")
        return []



def create_and_persist_chroma(
    texts: List[str],
    model_name: str,
    persist_directory: str,
) -> Chroma:
    """
    Creates a Chroma vector store from a list of texts using a specified embedding model
    and persists it to a directory.

    Args:
        texts: A list of texts to embed and store.
        model_name: The name of the Ollama model to use for embeddings.
        persist_directory: The directory where the Chroma vector store should be saved.

    Returns:
        A Chroma vector store instance.
    """
    try:
        # Initialize Ollama embeddings with the given model and show progress.
        embeddings = OllamaEmbeddings(model=model_name, show_progress=True)

        # Create a new Chroma vector store or load an existing one from the persistence directory.
        vectorstore = Chroma(
            persist_directory=persist_directory,
            embedding_function=embeddings,  # Use the initialized embeddings.
        )

        # Add texts to the vectorstore
        vectorstore.add_texts(texts=texts)

        # Persist the vector store to disk.  This is crucial for saving the data.
        vectorstore.persist()
        print(f"Chroma vector store persisted to {persist_directory} using model {model_name}")
        return vectorstore

    except Exception as e:
        print(f"Error creating or persisting Chroma vector store: {e}")
        raise  # Re-raise the exception to be handled by the caller



def read_data_from_ttl(file_path: str) -> List[str]:
    """
    Reads data from a TTL file, extracts relevant information, and returns it as a list of strings.

    This function assumes the TTL file contains information that can be converted into
    meaningful text for generating embeddings.  You'll need to adapt the query
    to match the structure of your TTL file.  This example extracts rdfs:label.

    Args:
        file_path (str): The path to the TTL file.

    Returns:
        List[str]: A list of strings extracted from the TTL file.
    """
    try:
        graph = Graph()
        graph.parse(file_path, format="ttl")

        # Modify this query to extract the data you need from your TTL file.
        # This example extracts the labels of all resources.
        query = """
            select * where {
                ?s ?p ?o .
            }
        """
        results = graph.query(query)

        texts = [str(row.s) for row in results]  # Extract the label strings.
        return texts

    except Exception as e:
        print(f"Error reading data from TTL file {file_path}: {e}")
        raise  # Re-raise to be handled in main()



def main(
         base_persist_directory: str = "./vectorstore/chroma_db-full-rdf",
         models: Optional[List[str]] = None,  # Changed to Optional[List[str]]
         ttl_file_path: str = "C:\\Users\\david\\Documents\\repositories\\llmind\\icd11_knowledge_graph.ttl"  # Added TTL file path
         ) -> None:
    """
    Main function to create and persist Chroma vector stores for ICD-11 prompts
    using different Ollama models, reading data from a TTL file.

    Args:
        base_persist_directory: Base directory where Chroma stores will be saved.
        models: List of Ollama model names to use.  If None, the user is prompted.
        ttl_file_path: The path to the TTL file.
    """
    try:
        # Read the prompts from the TTL file
        list_text = read_data_from_ttl(ttl_file_path)

        # Get available Ollama models
        available_models = get_available_ollama_models()
        print("Available Ollama Models:", available_models)

        # Determine the models to use
        if models is None:
            # Prompt the user to select a model
            print("Please enter the name of the Ollama model to use (or type 'list' to see available models):")
            def input_thread_func():
                global user_input
                user_input = input()

            input_thread = threading.Thread(target=input_thread_func)
            input_thread.daemon = True  # Allow the main thread to exit even if this thread is running.
            input_thread.start()

            input_thread.join(timeout=30)  # Wait for 30 seconds

            if input_thread.is_alive():
                print("No model selected within 30 seconds, using default model: gemma2:27b")
                model_name = "gemma2:27b"
            else:
                model_name = user_input

            if model_name.lower() == 'list':
                print("Available Ollama Models:", available_models)
                model_name = input("Please enter the name of the Ollama model to use: ") # ask again

            if model_name not in available_models:
                print(f"Model '{model_name}' is not available.  Using default model: gemma2:27b")
                model_name = "gemma2:27b"
            models_to_use = [model_name]
        else:
            models_to_use = models



        # Loop through each model in the list of models
        for model_name in models_to_use:
            # Construct the persistence directory for each model.
            persist_directory = f"{base_persist_directory}-{model_name.replace(':', '')}"

            # Filter out null or empty strings before passing to Chroma
            valid_texts = [text for text in list_text if text and text.strip()]
            if len(valid_texts) < len(list_text):
                print(f"Skipped {len(list_text) - len(valid_texts)} empty/null prompts before adding to Chroma with model {model_name}")


            # Create and persist the Chroma vector store.
            create_and_persist_chroma(
                texts=valid_texts,
                model_name=model_name,
                persist_directory=persist_directory,
            )

        print("Chroma vector store creation and persistence complete.")



    except Exception as e:
        # Catch any potential exceptions during the process.
        print(f"An unexpected error occurred: {e}")
        #  Consider more robust error handling here, such as logging or retrying.
        raise

if __name__ == "__main__":
    main(ttl_file_path="C:\\Users\\david\\Documents\\repositories\\llmind\\icd11_knowledge_graph.ttl") #make sure to change the file path
