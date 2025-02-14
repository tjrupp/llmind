import csv
import os
import time

from langchain_chroma import Chroma
from langchain_community.embeddings import OllamaEmbeddings
from langchain import hub
from langchain_community.llms import Ollama
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

# -------------------- Configuration -------------------- #

# Paths to input data, log file, and output directory base.
INPUT_CSV_PATH = '../data/input/DSM-5-TR_Clinical_Cases_splitted.csv'
LOG_FILE_PATH = '../data/log.txt'
OUTPUT_DIR_BASE = './output'
VECTORESTORE_BASE = './vectorstore'

# List of models to be processed.
MODELS = ["medllama2", "cniongolo/biomistral"]

# Total number of rows in the CSV (update if known dynamically)
TOTAL_ROWS = 103


# -------------------- Utility Functions -------------------- #

def format_docs(docs):
    """
    Formats a list of document objects by joining their `page_content` with double newlines.

    Args:
        docs (list): List of document objects with a `page_content` attribute.

    Returns:
        str: Formatted string combining all document contents.
    """
    return "\n\n".join(doc.page_content for doc in docs)


def setup_output_directory(model: str) -> str:
    """
    Ensures that an output directory exists for the given model.

    Args:
        model (str): The model name.

    Returns:
        str: Path to the model-specific output directory.
    """
    safe_model = model.replace(":", "")
    directory = os.path.join(OUTPUT_DIR_BASE, safe_model)
    os.makedirs(directory, exist_ok=True)
    return directory


def log_progress(message: str):
    """
    Appends a message to the log file.

    Args:
        message (str): The log message.
    """
    with open(LOG_FILE_PATH, "a", encoding="utf-8") as log_file:
        log_file.write(message + "\n")


# -------------------- Main Processing Function -------------------- #

def process_model(model: str):
    """
    Processes the input CSV for a specific model:
      - Initializes the vector store and LLM.
      - Builds the QA chain.
      - Iterates over each CSV row, invokes the chain, and writes output.

    Args:
        model (str): The model to use.
    """
    safe_model = model.replace(":", "")
    print("-------------------------------------------------------------")
    print(f"Processing model: {model}")

    # Initialize the vector store with the specified model and its embeddings.
    vectorstore = Chroma(
        persist_directory=os.path.join(VECTORESTORE_BASE, f"chroma_db-full-{safe_model}"),
        embedding_function=OllamaEmbeddings(model=model)
    )

    # Load the LLM model.
    llm = Ollama(model=model)

    # Convert the vector store into a retriever.
    retriever = vectorstore.as_retriever()

    # Load the RAG prompt from LangChain hub.
    rag_prompt = hub.pull("rlm/rag-prompt")

    # Build the QA chain pipeline.
    qa_chain = (
            {"context": retriever | format_docs, "question": RunnablePassthrough()}
            | rag_prompt
            | llm
            | StrOutputParser()
    )

    # Set up the output directory and CSV file for the current model.
    output_dir = setup_output_directory(model)
    output_csv_path = os.path.join(output_dir, "answers-cases.csv")

    # Open the output CSV file once and write the header.
    with open(output_csv_path, "w", encoding="utf-8", newline="") as output_file:
        writer = csv.writer(output_file, delimiter="§")
        writer.writerow(["row", "question", "answer"])

        start_time = time.time()  # Start time for processing rows.
        row_count = 0  # Row counter

        # Open and read the input CSV file.
        with open(INPUT_CSV_PATH, encoding="utf-8") as input_file:
            csv_reader = csv.reader(input_file, delimiter="§")
            # Skip header row.
            next(csv_reader, None)

            # Iterate over each row in the input CSV.
            for row in csv_reader:
                row_count += 1
                current_time = time.time()
                elapsed_time = current_time - start_time
                avg_time_per_row = elapsed_time / row_count
                estimated_total_time = avg_time_per_row * TOTAL_ROWS
                remaining_time = estimated_total_time - elapsed_time

                # Build a progress message.
                progress_message = (
                    f"Model: {model} -- Row {row_count}/{TOTAL_ROWS} -- "
                    f"Elapsed: {elapsed_time:.2f}s -- Estimated Total: {estimated_total_time:.2f}s -- "
                    f"Remaining: {remaining_time:.2f}s ({remaining_time / 3600:.2f}h)"
                )
                print(progress_message)
                log_progress(progress_message)

                try:
                    # Extract case information from the row.
                    # Adjust the index if needed. Here, we assume the case text is in the second column.
                    case_text = row[1] if len(row) > 1 else row[0]
                    question = f"Based on the ICD-11, make a diagnosis for this case: {case_text}"

                    # Invoke the QA chain to generate an answer.
                    answer = qa_chain.invoke(question)

                    # Clean up the answer by removing newline characters.
                    answer = answer.replace("\n", " ").strip()

                    # Write the processed row (row number, question, answer) to the output CSV.
                    writer.writerow([row_count, question, answer])

                except Exception as e:
                    # Log the exception details and write a placeholder row.
                    error_message = f"Error processing row {row_count}: {str(e)}"
                    log_progress(error_message)
                    writer.writerow([row_count, "no question", "NO ANSWER"])


# -------------------- Main Entry Point -------------------- #

def main():
    """
    Main function to iterate through all models and process the input CSV.
    """
    for model in MODELS:
        process_model(model)


if __name__ == "__main__":
    main()
