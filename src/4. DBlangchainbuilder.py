import pandas as pd
from langchain_community.embeddings import OllamaEmbeddings
from langchain_chroma import Chroma
from typing import List, Optional
import pyodbc  # Import pyodbc
import db_config  # Import your database configuration
import subprocess  # For running Ollama commands
import json
import time
import threading

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



def read_data_from_sql(connection_string: str, table_name: str) -> pd.DataFrame:
    """
    Reads data from a SQL Server table into a Pandas DataFrame.

    Args:
        connection_string (str): The SQL Server connection string.
        table_name (str): The name of the table to read from.

    Returns:
        pd.DataFrame: The data from the table.
    """
    try:
        cnxn = pyodbc.connect(connection_string)
        query = f"SELECT prompt FROM {table_name}"  # Only fetch the 'prompt' column
        df = pd.read_sql(query, cnxn)
        cnxn.close()
        return df
    except Exception as e:
        print(f"Error reading data from table {table_name}: {e}")
        raise  # Re-raise to be handled in main()



def main(
         base_persist_directory: str = "./vectorstore/chroma_db-full",
         models: Optional[List[str]] = None,  # Changed to Optional[List[str]]
         table_name: str = "ICD11_Final_Data"  # Added table name as parameter
         ) -> None:
    """
    Main function to create and persist Chroma vector stores for ICD-11 prompts
    using different Ollama models, reading data from a SQL Server table.

    Args:
        base_persist_directory: Base directory where Chroma stores will be saved.
        models: List of Ollama model names to use.  If None, the user is prompted.
        table_name: the name of the table to read the prompts from
    """
    try:
        # Read the prompts from the SQL Server database
        df = read_data_from_sql(db_config.SQL_SERVER_CONNECTION_STRING, table_name)


        # Extract the 'prompt' column from the DataFrame as a list of texts
        list_text = df["prompt"].tolist()  # Use tolist() for efficiency

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



    except KeyError as e:
        print(f"Error: Column 'prompt' not found in table. Ensure the table contains this column.  KeyError: {e}")
    except Exception as e:
        # Catch any other potential exceptions during the process.
        print(f"An unexpected error occurred: {e}")
        #  Consider more robust error handling here, such as logging or retrying.
        raise
    
if __name__ == "__main__":
    main()