from flask import Flask, request, jsonify
from langchain_chroma import Chroma
from langchain_community.embeddings import OllamaEmbeddings
from langchain import hub
from langchain_community.llms import Ollama
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
import pyodbc  # Import the pyodbc library
import importlib.util  # Import the importlib module
import re  # Import the re module for regular expressions
import db_config
app = Flask(__name__)

# Load database configuration from file
def load_db_config(config_file_path):
    try:
        # Load the module from the file path.
        spec = importlib.util.spec_from_file_location("db_config", config_file_path)
        db_config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(db_config)
        connection_string = db_config.SQL_SERVER_CONNECTION_STRING
        if not connection_string:
            raise ValueError("Missing SQL_SERVER_CONNECTION_STRING in db_config.py")
        return connection_string
    except FileNotFoundError:
        raise FileNotFoundError(f"Error: Database configuration file not found at {config_file_path}")
    except AttributeError:
        raise AttributeError(
            f"Error: Could not find SQL_SERVER_CONNECTION_STRING in {config_file_path}.  Make sure it is defined correctly.")
    except Exception as e:
        raise Exception(f"An unexpected error occurred while reading the database configuration: {e}")


# Function to fetch differential diagnoses from the database
def get_differential_diagnoses(conn, initial_diagnosis, confidence_interval=0.8):
    """
    Fetches differential diagnoses from the SQL Server database based on the initial diagnosis,
    using a confidence interval for fuzzy matching.

    Args:
        conn (pyodbc.Connection): The database connection object.
        initial_diagnosis (str): The initial diagnosis from the LLM.
        confidence_interval (float, optional): The minimum similarity score (0 to 1) for a match.
            Defaults to 0.8.

    Returns:
        list: A list of dictionaries, where each dictionary represents a differential diagnosis.
              Returns an empty list if no differential diagnoses are found.
    """
    try:
        cursor = conn.cursor()
        # SQL query to fetch differential diagnoses
        query = """
            SELECT 
                [level], 
                [type], 
                [value],
                [parent_condition]
            FROM 
                [dbo].[DiagnosisData]
            WHERE 
                [level_0_info] LIKE ?
            ORDER BY 
                [level];  -- Order by level for a more organized result
        """
        # Convert the initial diagnosis to a SQL Server compatible pattern.
        # Use শব্দের মত শব্দাংশ to make it more permissive
        pattern = f"%{initial_diagnosis}%"
        cursor.execute(query, pattern)
        results = cursor.fetchall()

        # Process the results into a list of dictionaries
        diagnoses = []
        for row in results:
            diagnoses.append({
                "level": row.level,
                "type": row.type,
                "value": row.value,
                "parent_condition": row.parent_condition
            })
        return diagnoses
    except pyodbc.Error as e:
        print(f"Error fetching differential diagnoses: {e}")
        return []  # Return an empty list in case of error
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []


def get_next_question(diagnoses, current_level, previous_answer=None):
    """
    Determines the next question to ask the user based on the differential diagnoses.

    Args:
        diagnoses (list): A list of dictionaries representing the differential diagnoses.
        current_level (int): The current level in the diagnosis graph.
        previous_answer (str, optional): The user's answer to the previous question ("yes" or "no").

    Returns:
        tuple: (str, int) - The next question to ask, and the next level, or (None, current_level) if no further questions.
    """
    next_level = current_level + 1
    possible_questions = []

    for diagnosis in diagnoses:
        if diagnosis['level'] == next_level:
            if previous_answer is None or diagnosis['type'] == previous_answer:
                if diagnosis['type'] == 'condition':
                    return diagnosis['value'], next_level
                elif diagnosis['level'] == next_level:
                    possible_questions.append(diagnosis)

    if possible_questions:
        # Prioritize questions
        for question in possible_questions:
            if question['type'] == 'condition':
                return question['value'], next_level

    return None, current_level  # No suitable question found


@app.route('/askLLM', methods=['POST'])
def askLLM():
    """
    Endpoint that takes a clinical case as input, queries the LLM, and guides the user
    through a series of questions to refine the differential diagnosis.
    """
    # Get the "input_string" parameter from the JSON request body
    input_data = request.get_json()
    input_string = input_data.get('input_string')
    previous_answers = input_data.get('previous_answers', [])  # Get previous answers

    # Return an error if "input_string" is missing
    if input_string is None:
        return jsonify({"error": "Missing 'input_string' parameter"}), 400

    # Model name to be used
    modello = "gemma2:27b"

    # Initialize the vector store with the specified model
    vectorstore = Chroma(persist_directory=f"./vectorstore/chroma_db-full-{modello.replace(':','')}",
                        embedding_function=OllamaEmbeddings(model=modello))

    # Load the Llama3 model
    llm = Ollama(model=modello)

    # Use the vector store as the retriever
    retriever = vectorstore.as_retriever()

    # Function to format documents for the QA chain
    def format_docs(docs):
        return "\n\n".join(doc.page_content for doc in docs)

    # Load the QA chain prompt from langchain hub
    rag_prompt = hub.pull("rlm/rag-prompt")

    # Create the QA chain
    qa_chain = (
        {"context": retriever | format_docs, "question": RunnablePassthrough()}
        | rag_prompt
        | llm
        | StrOutputParser()
    )

    # Prepare the question from the input string
    question = f"{input_string}"

    # Invoke the QA chain to get the answer
    answer = qa_chain.invoke(question)
    initial_diagnosis = answer.replace("\n", "")

    # Get differential diagnoses from the database
    try:
        conn = pyodbc.connect(db_config.SQL_SERVER_CONNECTION_STRING) # Use the connection string from db_config
        differential_diagnoses = get_differential_diagnoses(conn, initial_diagnosis, confidence_interval=0.7) # Set confidence interval
    except Exception as e:
        print(f"Error: {e}")
        conn = None  # Set conn to None to avoid using an invalid connection
        differential_diagnoses = []  # Ensure that differential_diagnoses is always defined

    # if not differential_diagnoses:
    #    if conn:
    #      conn.close()
    #    return jsonify({"output_string": initial_diagnosis, "differential_diagnoses": []})

    # Start interactive questioning
    current_level = 0
    final_diagnosis = None

    for previous_answer in previous_answers:
        next_question, current_level = get_next_question(differential_diagnoses, current_level, previous_answer)
        if next_question is None:
            break

    next_question, current_level = get_next_question(differential_diagnoses, current_level)

    if next_question is None:
        # If no more questions, get the final diagnosis
        for diagnosis in differential_diagnoses:
            if diagnosis['level'] == current_level + 1:
                final_diagnosis = diagnosis['value']
                break
        if conn:
            conn.close()
        return jsonify(
            {"output_string": initial_diagnosis, "final_diagnosis": final_diagnosis,
             "differential_diagnoses": differential_diagnoses})  # Return the final diagnosis

    # Return the next question
    if conn:
        conn.close()
    return jsonify(
        {"output_string": initial_diagnosis, "next_question": next_question, "current_level": current_level,
         "differential_diagnoses": differential_diagnoses})
    

if __name__ == '__main__':
    # Run the server
    app.run(host="0.0.0.0", debug=True)
