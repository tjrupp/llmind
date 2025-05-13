import requests
import json
from typing import List, Dict
import pyodbc  # For connecting to SQL Server
import db_config
import pyodbc
import re
import random

# Output file for the TTL data
TTL_FILE = "icd11_knowledge_graph.ttl"
# Output file for the HTML visualization
HTML_FILE = "icd11_taxonomy.html"

# SQL Server connection string
#SQL_SERVER_CONNECTION_STRING = "Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=llmind;Uid=sa;Pwd=LLMind2025!;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"

# -----------------------------------------------------------------------------
# Configuration Section
# -----------------------------------------------------------------------------

# API base URL and endpoint templates
BASE_URI_TEMPLATE = 'http://localhost/icd/release/11/2025-01/mms/{}?include=diagnosticCriteria'
ROOT_URI = 'http://localhost/icd/release/11/2025-01/mms'

# HTTP headers for API requests
HEADERS = {
    'Accept': 'application/json',
    'Accept-Language': 'en',
    'API-Version': 'v2'
}



# Database connection string.  **Replace with your actual connection details.**
# Important:  Store this securely, e.g., in environment variables.  DO NOT hardcode in production code.
# Example (using Windows Authentication - trusted connection):
# SQL_SERVER_CONNECTION_STRING = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server_name;DATABASE=your_database_name;TRUSTED_CONNECTION=yes;"
# Example (using SQL Server Authentication):
SQL_SERVER_CONNECTION_STRING = db_config.SQL_SERVER_CONNECTION_STRING

# Note:  You might need to adjust the DRIVER.  "ODBC Driver 17 for SQL Server" is common, but check your system.

# -----------------------------------------------------------------------------
# Data Retrieval Functions
# -----------------------------------------------------------------------------

def retrieve_code(uri: str, session: requests.Session, results: List[Dict], parent_id: str,fll :int) -> None:
    """
    Recursively retrieves ICD code data from the API starting at the given URI.
    Processes each node: if the node is a 'category' without children (i.e. a leaf node),
    its details are saved into the results list. If the node contains children, each child
    URI is constructed and processed recursively.

    Args:
        uri (str): The API endpoint URI for the current ICD node.
        session (requests.Session): A persistent session for HTTP requests.
        results (List[Dict]): A list to accumulate the processed ICD entries.
    """
    try:
        response = session.get(uri, headers=HEADERS, verify=False)
        response.raise_for_status()  # Raise an error for HTTP error codes
        data = response.json()
    except Exception as e:
        print(f"Error retrieving data from {uri}: {e}")
        return

    # If this node is a leaf category (i.e., it has no 'child' field), process and record it.
    if data.get('classKind') == 'category' and 'child' not in data:
        entry = {
            'code': data.get('code', '').replace(";", "~"),
            'title': data.get('title', {}).get('@value', '').replace(";", "~"),
            'definition': data.get('definition', {}).get('@value', '').replace(";", "~"),
            'longdefinition': data.get('longdefinition', {}).get('@value', '').replace(";", "~"),
            'inclusions': "; ".join([inc.get('label', {}).get('@value', '').replace(";", "~") for inc in data.get('inclusion', [])]),
            'exclusions': "; ".join([exc.get('label', {}).get('@value', '').replace(";", "~") for exc in data.get('exclusion', [])]),
            'diagnosticCriteria': data.get('diagnosticCriteria', {}).get('@value', '').replace(";", "~"),
            'category_code': parent_id,  # Add the category code here
        }
        results.append(entry)

    # If the node contains children, build their URIs and recursively process each one.
    if 'child' in data:
        hierarchy = []
        fll += random.randint(1, 99999)
        parent_id = extract_hierarchy(data, hierarchy,fll,parent_id)
        for child in data['child']:
            # The unique identifier is extracted from the child URL string.
            child_id = child.split("/mms/")[-1]
            child_uri = BASE_URI_TEMPLATE.format(child_id)
            retrieve_code(child_uri, session, results,parent_id,fll)


def extract_hierarchy(data, hierarchy,fll,parentId):
    """
    Extracts the hierarchical information (code, title, definition) from the ICD-11 data.

    Args:
        data (dict): The ICD-11 data for a single entity.
        hierarchy (list): The list to store hierarchy at each level
    """
    if not data:
        return
    
    code = data.get('code', '')
    if code == '':
        code = 'FLL'+str(fll)
    # Extract the code
    # Extract the code, 
    title = data.get('title', {}).get('@value', '')
    definition = data.get('definition', {}).get('@value', '')
    try:
        conn = pyodbc.connect(SQL_SERVER_CONNECTION_STRING)
        cursor = conn.cursor()
        # Use executemany for efficient insertion of multiple rows
        cursor.execute(
            f"INSERT INTO llmind.dbo.ICD11_Categories (code, title, definition, parent) VALUES ('{code}', '{title.replace("'",'')}', '{definition.replace("'",'')}','{parentId}')",
        )
        conn.commit()
        conn.close()
    except pyodbc.Error as e:
        print(f"Error inserting diagnostic categories into the database: {e}")
        print(f"INSERT INTO llmind.dbo.ICD11_Categories (code, title, definition, parent) VALUES ('{code}', '{title.replace("'",'')}', '{definition.replace("'",'')}','{parentId}')")

    hierarchy.append({'code': code, 'title': title, 'definition': definition})
    return code



# -----------------------------------------------------------------------------
# Database Interaction Functions
# -----------------------------------------------------------------------------

def create_table_if_not_exists(connection_string: str) -> None:
    """
    Creates the ICD-11 table in the SQL Server database if it does not already exist.

    Args:
        connection_string (str): The connection string for the SQL Server database.
    """
    try:
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        # Check if the table exists
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = 'ICD11_Codes'")
        table_exists = cursor.fetchone()

        if not table_exists:
            # Create the table with appropriate data types.  Use NVARCHAR for Unicode support.
            # Create the ICD11_Categories table
            cursor.execute("""
                CREATE TABLE ICD11_Categories (
                    code NVARCHAR(255) PRIMARY KEY,
                    title NVARCHAR(MAX),
                    definition NVARCHAR(MAX),
                    parent NVARCHAR(255) -- Add 
                )
            """)
            cnxn.commit()
            print("ICD11_Categories table created successfully.")

            cursor.execute("""
                CREATE TABLE ICD11_Codes (
                    code NVARCHAR(255) PRIMARY KEY,
                    title NVARCHAR(MAX),
                    definition NVARCHAR(MAX),
                    longdefinition NVARCHAR(MAX),
                    inclusions NVARCHAR(MAX),
                    exclusions NVARCHAR(MAX),
                    diagnosticCriteria NVARCHAR(MAX),
                    category_code NVARCHAR(255),  -- Add category code
                    FOREIGN KEY (category_code) REFERENCES ICD11_Categories(code) --FK
                )
            """)
            cnxn.commit()
            print("ICD11_Codes table created successfully.")


        else:
            print("ICD11_Codes and ICD11_Categories tables already exist.")
        cursor.close()
        cnxn.close()
    except Exception as e:
        print(f"Error creating table: {e}")
        # Consider raising the exception or handling it more robustly (e.g., logging, retrying).
        raise  # Re-raise to stop execution if table creation fails.

def insert_data_into_table(connection_string: str, data: List[Dict]) -> None:
    """
    Inserts the ICD-11 code data into the SQL Server table.

    Args:
        connection_string (str): The connection string for the SQL Server database.
        data (List[Dict]): A list of dictionaries, where each dictionary represents an ICD-11 code entry.
    """
    try:
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        # Use a parameterized query to prevent SQL injection.
        sql = """
            INSERT INTO ICD11_Codes (code, title, definition, longdefinition, inclusions, exclusions,diagnosticCriteria, category_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        for row in data:
            try:
                cursor.execute(sql, (
                    row['code'],
                    row['title'],
                    row['definition'],
                    row['longdefinition'],
                    row['inclusions'],
                    row['exclusions'],
                    row['diagnosticCriteria'],
                    row.get('category_code')  # Include the category code in the insert
                ))
            except pyodbc.Error as e:
                print(f"Error inserting row: {e}.  Row data: {row}")
                cnxn.rollback() # Rollback the current transaction on error.
                continue #  Consider whether to continue or stop on error. Here I continue to the next row.

        cnxn.commit()
        print(f"{len(data)} rows successfully inserted into ICD11_Codes.")
        cursor.close()
        cnxn.close()
    except Exception as e:
        print(f"Error inserting data: {e}")
        #  Consider more sophisticated error handling (e.g., logging, retrying)
        raise  # Re-raise to stop execution after logging


def get_icd11_data_from_db(connection_string):
    """
    Retrieves ICD-11 data from the SQL Server database.

    Args:
        connection_string: The connection string for the SQL Server database.

    Returns:
        A list of dictionaries, where each dictionary represents an ICD-11 entity,
        or None on error.
    """
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        # Query to fetch all data from the ICD11_Codes table, including diagnosticCriteria
        query = "SELECT cd.code as code, cd.title as title, cd.definition as definition, longdefinition, inclusions, exclusions, diagnosticCriteria, category_code, ct.title as parent, ct2.title as secondParent FROM llmind.dbo.ICD11_Codes cd  left join ICD11_Categories ct on cd.category_code = ct.code left join ICD11_Categories ct2 on ct.parent = ct2.code"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]  # Get column names
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))  # Convert row to dictionary
        conn.close()
        return results
    except pyodbc.Error as e:
        print(f"Error connecting to or querying the database: {e}")
        return None



def insert_diagnostic_criteria_into_db(connection_string, criteria_data):
    """
    Inserts diagnostic criteria into the ICD11_DiagnosticCriteria table.

    Args:
        connection_string: The SQL Server connection string.
        criteria_data: A list of tuples, where each tuple contains (code, criterion_type, criterion_text).
    """
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        # Use executemany for efficient insertion of multiple rows
        cursor.executemany(
            "INSERT INTO llmind.dbo.ICD11_DiagnosticCriteria (code, criterion_type, criterion_text) VALUES (?, ?, ?)",
            criteria_data
        )
        conn.commit()
        conn.close()
    except pyodbc.Error as e:
        print(f"Error inserting diagnostic criteria into the database: {e}")



def parse_diagnostic_criteria(diagnostic_criteria_text):
    """
    Parses the diagnostic criteria text into its sub-parts.

    Args:
        diagnostic_criteria_text: The string containing the diagnostic criteria.

    Returns:
        A list of tuples, where each tuple contains (code, criterion_type, criterion_text).
        Returns an empty list if no criteria are found or the input is invalid.
    """

    if not diagnostic_criteria_text:
        return []

    criteria = []
    # Split by headings like "## Essential (Required) Features:"
    parts = re.split(r"##\s*([A-Za-z\s()]+):", diagnostic_criteria_text)
    # The first element of parts will be the text before the first heading, which we can ignore
    for i in range(1, len(parts), 2):
        criterion_type = parts[i].strip()
        criterion_text = parts[i+1].strip()
        # Split the text into individual criteria, handling bullet points and newlines
        sub_criteria = re.split(r"\n\s*[-–—]\s*", criterion_text)
        for sub_criterion in sub_criteria:
            sub_criterion = sub_criterion.strip()
            if sub_criterion:  # Avoid empty strings
                criteria.append((criterion_type, sub_criterion))
    return criteria



def extract_symptoms(definition, longdefinition):
    """
    Extracts symptoms from the definition and long definition, standardizing the output.

    Args:
        definition: The definition string.
        longdefinition: The long definition string.

    Returns:
        A list of symptom strings, standardized according to the requirements.
    """
    symptoms = []
    text = f"{definition} {longdefinition}"

    symptom_keywords = [
        "symptoms include", "symptoms are", "characterized by", "manifested by",
        "presents with", "associated with", "signs and symptoms of", "features include"
    ]
    for keyword in symptom_keywords:
        match = re.search(rf"{keyword}\s*([\w\s,()-]+?)(?:\.|$)", text, re.IGNORECASE)
        if match:
            symptom_string = match.group(1).strip()
            # Split by commas and "or"
            for s in re.split(r",\s*|or\s*", symptom_string):
                s = s.strip()
                if s:
                    # Remove articles and standardize case
                    s = re.sub(r"^(a|an|the)\s+", "", s, flags=re.IGNORECASE)
                    # Remove first letter and space
                    if len(s) > 2 and s[1:2] == ' ':
                        s = s[2:]
                    s = s.capitalize()
                    if len(s) <= 30 and len(s) > 2 and s[-1].isalpha():  # Check length and last char
                        symptoms.append(s)

    # Secondary check using the provided symptom list
    additional_symptoms = [
        "Acid and chemical burns", "Allergies", "Animal and human bites", "Ankle problems",
        "Back problems", "Bites and stings", "Blisters", "Bowel incontinence", "Breast pain",
        "Breast swelling in men", "Breathlessness and cancer", "Burns and scalds",
        "Calf problems", "Cancer-related fatigue", "Catarrh", "Chronic pain", "Constipation",
        "Cold sore", "Cough", "Cuts and grazes", "Chest pain", "Dehydration", "Diarrhoea",
        "Dizziness (lightheadedness)", "Dry mouth", "Earache", "Eating and digestion with cancer",
        "Elbow problems", "Farting", "Feeling of something in your throat (Globus)",
        "Fever in adults", "Fever in children", "Flu", "Foot problems", "Genital symptoms",
        "Hair loss and cancer", "Hay fever", "Headaches", "Hearing loss", "Hip problems",
        "Indigestion", "Itchy bottom", "Itchy skin", "Knee problems", "Living well with COPD",
        "Living with chronic pain", "Migraine", "Mouth ulcer", "Neck problems", "Nipple discharge",
        "Nipple inversion (inside out nipple)", "Nosebleed", "Pain and cancer", "Skin rashes in children",
        "Shortness of breath", "Shoulder problems", "Skin rashes in children", "Soft tissue injury advice",
        "Sore throat", "Stomach ache and abdominal pain", "Sunburn", "Swollen glands",
        "Testicular lumps and swellings", "Thigh problems", "Tick bites", "Tinnitus", "Toothache",
        "Urinary incontinence", "Urinary incontinence in women", "Urinary tract infection (UTI)",
        "Urinary tract infection (UTI) in children", "Vaginal discharge", "Vertigo",
        "Vomiting in adults", "Vomiting in children and babies", "Warts and verrucas",
        "Wrist, hand and finger problems",
        "Recurrent unexpected panic attacks",
        "Panic attacks not restricted to particular stimuli or situations",
        "Discrete episodes of intense fear or apprehension",
        "Rapid onset of symptoms",
        "Concurrent onset of several characteristic symptoms",
        "Palpitations or increased heart rate",
        "Sweating",
        "Trembling",
        "Shortness of breath",
        "Chest pain",
        "Dizziness or lightheadedness",
        "Chills",
        "Hot flushes",
        "Fear of imminent death",
        "Persistent concern about the recurrence of panic attacks",
        "Persistent concern about the significance of panic attacks",
        "Behaviours intended to avoid the recurrence of panic attacks",
        "Significant impairment in personal functioning",
        "Significant impairment in family functioning",
        "Significant impairment in social functioning",
        "Significant impairment in educational functioning",
        "Significant impairment in occupational functioning",
        "Numbness",
        "Tightness",
        "Tingling",
        "Burning",
        "Pain",
        "Dangerously low body weight",
        "Weight loss induced through restricted food intake",
        "Maintenance of low body weight through restricted food intake",
        "Weight loss induced through fasting",
        "Maintenance of low body weight through fasting",
        "Weight loss induced through a combination of restricted food intake and increased energy expenditure",
        "Maintenance of low body weight through a combination of restricted food intake and increased energy expenditure",
        "Weight loss induced through excessive exercise",
        "Maintenance of low body weight through excessive exercise",
        "Absence of binge eating behaviours",
        "Absence of purging behaviours",
        "Marked symptoms of anxiety persisting for at least several months",
        "Anxiety present for more days than not",
        "General apprehension ('free-floating anxiety')",
        "Excessive worry focused on multiple everyday events",
        "Worry often concerning family",
        "Worry often concerning health",
        "Worry often concerning finances",
        "Worry often concerning school or work",
        "Muscular tension",
        "Motor restlessness",
        "Sympathetic autonomic over-activity",
        "Subjective experience of nervousness",
        "Difficulty maintaining concentration",
        "Irritability",
        "Sleep disturbance",
        "Significant distress",
        "Significant impairment in personal functioning",
        "Significant impairment in family functioning",
        "Significant impairment in social functioning",
        "Significant impairment in educational functioning",
        "Significant impairment in occupational functioning",
        "Difficulties in the acquisition and comprehension of complex language concepts",
        "Difficulties in the acquisition of academic skills",
        "Limited language",
        "Limited capacity for acquisition of academic skills",
        "Motor impairments",
        "Very limited communication abilities",
        "Restricted capacity for acquisition of basic concrete skills",
        "Co-occurring motor and sensory impairments",
        "Difficulties in the acquisition of speech",
        "Difficulties in the production of speech",
        "Difficulties in the perception of speech",
        "Errors of pronunciation (in number or types)",
        "Reduced intelligibility of speech",
        "Frequent disruption of the normal rhythmic flow of speech",
        "Pervasive disruption of the normal rhythmic flow of speech",
        "Repetitions in sounds",
        "Repetitions in syllables",
        "Repetitions in words",
        "Repetitions in phrases",
        "Prolongations in sounds",
        "Prolongations in syllables",
        "Prolongations in words",
        "Prolongations in phrases",
        "Blocking (speech)",
        "Word avoidance",
        "Word substitutions",
        "Persistent difficulties in the acquisition of language",
        "Persistent difficulties in the understanding of language",
        "Persistent difficulties in the production of language",
        "Persistent difficulties in the use of language",
        "Markedly below expected level of receptive language (understanding spoken or signed language)",
        "Persistent impairment in expressive language (producing and using spoken or signed language)",
        "Markedly below expected level of expressive language (producing and using spoken or signed language)",
        "Persistent and marked difficulties with the understanding of language in social contexts (e.g., making inferences, understanding verbal humour, resolving ambiguous meaning)",
        "Persistent and marked difficulties with the use of language in social contexts",
        "Significant difficulties in learning word reading accuracy",
        "Significant difficulties in learning reading fluency",
        "Significant difficulties in learning reading comprehension",
        "Significant difficulties in learning spelling accuracy",
        "Significant difficulties in learning grammar and punctuation accuracy",
        "Significant difficulties in learning organisation and coherence of ideas in writing",
        "Significant difficulties in learning number sense",
        "Significant difficulties in the memorization of number facts",
        "Significant difficulties in accurate calculation",
        "Significant difficulties in fluent calculation",
        "Significant difficulties in accurate mathematical reasoning",
        "Significant difficulties in learning academic skills other than reading, mathematics, and written expression",
        "Significant delay in the acquisition of gross motor skills",
        "Significant delay in the acquisition of fine motor skills",
        "Impairment in the execution of coordinated motor skills",
        "Clumsiness",
        "Slowness of motor performance",
        "Inaccuracy of motor performance",
        "Difficulty in sustaining attention to tasks that do not provide a high level of stimulation",
        "Difficulty in sustaining attention to tasks that do not provide frequent rewards",
        "Distractibility",
        "Problems with organisation",
        "Some hyperactive-impulsive symptoms",
        "Excessive motor activity (hyperactivity)",
        "Difficulties with remaining still (hyperactivity)",
        "Impulsivity (tendency to act without deliberation)",
        "Lack of consideration of risks and consequences (impulsivity)",
        "Some inattentive symptoms",
        "Voluntary repetitive movements",
        "Stereotyped movements",
        "Apparently purposeless movements",
        "Often rhythmic movements",
        "Body rocking",
        "Head rocking",
        "Finger-flicking mannerisms",
        "Hand flapping",
        "Self-injurious behaviours",
        "Head banging",
        "Face slapping",
        "Eye poking",
        "Biting of the hands",
        "Biting of the lips",
        "Biting of other body parts",
        "Persistent delusions",
        "Persistent hallucinations (most commonly verbal auditory hallucinations)",
        "Disorganised thinking (formal thought disorder)",
        "Loose associations",
        "Thought derailment",
        "Incoherence",
        "Grossly disorganised behaviourBehaviour that appears bizarre",
        "Purposeless behaviour",
        "Non-goal-directed behaviour",
        "Experiences of passivity and controlFeeling that one's feelings are under external control",
        "Feeling that one's impulses are under external control",
        "Feeling that one's thoughts are under external control",
        "Constricted affect",
        "Blunted affect",
        "Flat affect",
        "Alogia (paucity of speech)",
        "Avolition (general lack of drive)",
        "Lack of motivation to pursue meaningful goals (avolition)",
        "Asociality (reduced or absent engagement with others)",
        "Reduced interest in social interaction (asociality)",
        "Anhedonia (inability to experience pleasure)",
        "Depressed mood (feeling down, sad)",
        "Tearfulness (sign of depressed mood)",
        "Defeated appearance (sign of depressed mood)",
        "Elevated mood",
        "Euphoric mood",
        "Irritable mood",
        "Expansive mood",
        "Rapid changes among different mood states (mood lability)",
        "Increased subjective experience of energy",
        "Increased goal-directed activity",
        "Psychomotor agitationExcessive motor activity",
        "Purposeless behaviours",
        "Fidgeting",
        "Shifting",
        "Fiddling",
        "Inability to sit or stand still",
        "Wringing of the hands",
        "Psychomotor retardationVisible generalised slowing of movements",
        "Slowing of speech",
        "Catatonic symptomsExcitement",
        "Posturing",
        "Waxy flexibility",
        "Negativism",
        "Mutism",
        "Stupor",
        "Impairment in speed of processing",
        "Impairment in attention/concentration",
        "Impairment in orientation",
        "Impairment in judgment",
        "Impairment in abstraction",
        "Impairment in verbal learning",
        "Impairment in visual learning",
        "Impairment in working memory",
        "Euphoria",
        "Irritability",
        "Expansiveness",
        "Increased activity",
        "Rapid speech",
        "Pressured speech",
        "Flight of ideas",
        "Increased self-esteem",
        "Grandiosity",
        "Decreased need for sleep",
        "Distractibility",
        "Impulsive behaviour",
        "Reckless behaviour",
        "Mild elevation of mood",
        "Rapid or racing thoughts",
        "Increase in sexual drive",
        "Increase in sociability",
        "Depressed mood",
        "Diminished interest in activities",
        "Difficulty concentrating",
        "Feelings of worthlessness",
        "Excessive or inappropriate guilt",
        "Hopelessness",
        "Recurrent thoughts of death",
        "Recurrent thoughts of suicide",
        "Changes in appetite",
        "Changes in sleep",
        "Reduced energy",
        "Fatigue",
        "Presence of several prominent manic symptoms",
        "Presence of several prominent depressive symptoms",
        "Symptoms occur simultaneously or alternate very rapidly",
        "Altered mood state (depressed, dysphoric, euphoric, or expansive)",
        "Persistent instability of mood",
        "Eccentricities in behaviour",
        "Eccentricities in appearance",
        "Eccentricities in speech",
        "Cognitive distortions",
        "Perceptual distortions",
        "Unusual beliefs",
        "Discomfort with interpersonal relationships",
        "Reduced capacity for interpersonal relationships",
        "Constricted affect",
        "Inappropriate affect",
        "Paranoid ideas",
        "Ideas of reference",
        "Other psychotic symptoms",
        "Hallucinations in any modality",
        "Symptoms of decreased psychomotor activity",
        "Symptoms of increased psychomotor activity",
        "Symptoms of abnormal psychomotor activity"
    ]
    for symptom in additional_symptoms:
        if re.search(rf"\b{re.escape(symptom)}\b", text, re.IGNORECASE):
             # Remove articles and standardize case for additional symptoms too
            symptom = re.sub(r"^(a|an|the)\s+", "", symptom, flags=re.IGNORECASE)
            # Remove first letter and space
            if len(symptom) > 2 and symptom[1:2] == ' ':
                symptom = symptom[2:]
            s = symptom.capitalize() # Fix: Assign to s before the length check
            if len(s) <= 30 and len(s) > 2 and s[-1].isalpha():
                symptoms.append(s)

    # Split symptoms containing "or" into separate entries
    split_symptoms = []
    for symptom in symptoms:
        if "or" in symptom:
            parts = symptom.split("or")
            for part in parts:
                part = part.strip()
                if part and len(part) <= 30 and len(part) > 2 and part[-1].isalpha():
                  split_symptoms.append(part)
        else:
            split_symptoms.append(symptom)

    return list(set(split_symptoms))  # Remove duplicates after splitting



def insert_symptoms_into_db(connection_string, symptoms_data):
    """
    Inserts symptoms into the ICD11_Symptoms table.

    Args:
        connection_string: The SQL Server connection string.
        symptoms_data: A list of tuples, where each tuple contains (code, symptom_text).
    """
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        # Use executemany for efficient insertion
        cursor.executemany(
            "INSERT INTO llmind.dbo.ICD11_Symptoms (code, symptom_text) VALUES (?, ?)",
            symptoms_data
        )
        conn.commit()
        conn.close()
    except pyodbc.Error as e:
        print(f"Error inserting symptoms into the database: {e}")



# def generate_ttl(icd11_data, diagnostic_criteria_data, symptoms_data, prescriptions_data):
#     """
#     Generates TTL triples from the ICD-11 data, diagnostic criteria, symptoms, and prescriptions.

#     Args:
#         icd11_data: A list of dictionaries, where each dictionary represents an ICD-11 entity.
#         diagnostic_criteria_data: A dictionary where keys are ICD-11 codes and values are
#             lists of dictionaries, each representing a diagnostic criterion.
#         symptoms_data: A dictionary where keys are ICD-11 codes and values are lists of
#             symptom strings.
#         prescriptions_data: A dictionary where keys are ICD-11 codes and values are lists of
#             drug prescription strings.
#     Returns:
#         A string containing the TTL triples. Returns an empty string if data is None or empty.
#     """
#     if not icd11_data:
#         return ""

#     ttl_triples = []

#     for entity_data in icd11_data:
#         entity_id = entity_data.get("code")
#         if not entity_id:
#             print(f"Entity has no code: {entity_data}")
#             continue  # Skip this entity

#         entity_uri = f"icd:{entity_id}"

#         # Add type information.  For simplicity, assume all are icd:Disease.
#         ttl_triples.append(f"<{entity_uri}> rdf:type icd:Disease .")

#         title = entity_data.get("title")
#         if title:
#             escaped_title = title.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
#             ttl_triples.append(f"<{entity_uri}> icd:title \"{escaped_title}\" .")

#         definition = entity_data.get("definition")
#         if definition:
#             escaped_definition = definition.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
#             ttl_triples.append(f"<{entity_uri}> icd:definition \"{escaped_definition}\" .")
        
#         longdefinition = entity_data.get("longdefinition")
#         if longdefinition:
#             escaped_longdefinition = longdefinition.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
#             ttl_triples.append(f"<{entity_uri}> icd:longdefinition \"{escaped_longdefinition}\" .")

#         # Inclusions and exclusions are lists in the database, handle them
#         inclusions = entity_data.get("inclusions")
#         if inclusions:
#             escaped_inclusions = inclusions.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
#             ttl_triples.append(f"<{entity_uri}> icd:inclusions \"{escaped_inclusions}\" .")

#         exclusions = entity_data.get("exclusions")
#         if exclusions:
#             escaped_exclusions = exclusions.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
#             ttl_triples.append(f"<{entity_uri}> icd:exclusions \"{escaped_exclusions}\" .")

#         parent = entity_data.get("parent")
#         if parent:
#             parent_exclusions = parent.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
#             ttl_triples.append(f"<{entity_uri}> icd:hasParent \"{parent_exclusions}\" .")    

#         secondParent = entity_data.get("secondParent")
#         if secondParent:
#             secondParent_exclusions = secondParent.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
#             ttl_triples.append(f"<{entity_uri}> icd:hasSecondParent \"{secondParent_exclusions}\" .")        

#         # Handle diagnostic criteria from the separate table
#         if entity_id in diagnostic_criteria_data:
#             for criterion in diagnostic_criteria_data[entity_id]:
#                 criterion_type = criterion['type']
#                 criterion_text = criterion['text']
#                 escaped_text = criterion_text.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
#                 ttl_triples.append(f"<{entity_uri}> icd:hasCriterion [ rdf:type <http://id.who.int/icd/property/diagnosticCriterion> ;"
#                                    f" diag:criterionType \"{criterion_type}\" ;"
#                                    f" diag:criterionText \"{escaped_text}\" ] .")
        
#         # Handle symptoms
#         if entity_id in symptoms_data:
#             for symptom in symptoms_data[entity_id]:
#                 escaped_symptom = symptom.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
#                 ttl_triples.append(f"<{entity_uri}> icd:hasSymptom \"{escaped_symptom}\" .")

#         # Handle prescriptions
#         if entity_id in prescriptions_data:
#             for prescription in prescriptions_data[entity_id]:
#                 escaped_prescription = prescription.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
#                 ttl_triples.append(f"<{entity_uri}> icd:treatment \"{escaped_prescription}\" .")

#         #  The SQL table doesn't have parent/child, so we can't represent the hierarchy.
#         #  If your SQL Server table *does* have parent/child relationships, you'll need to
#         #  modify the SQL query and this function to handle that.

#     return "\n".join(ttl_triples)


def get_diagnostic_criteria_from_db(connection_string):
    """
    Retrieves diagnostic criteria from the SQL Server database's ICD11_DiagnosticCriteria table.

    Args:
        connection_string: The connection string for the SQL Server database.

    Returns:
        A dictionary where keys are ICD-11 codes and values are lists of dictionaries,
        each representing a diagnostic criterion.  Returns None on error.
    """
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        query = "SELECT code, criterion_type, criterion_text FROM llmind.dbo.ICD11_DiagnosticCriteria"
        cursor.execute(query)
        results = {}
        for row in cursor.fetchall():
            code, criterion_type, criterion_text = row
            if code not in results:
                results[code] = []
            results[code].append({'type': criterion_type, 'text':criterion_text})
        conn.close()
        return results
    except pyodbc.Error as e:
        print(f"Error connecting to or querying the database for diagnostic criteria: {e}")
        return None


def get_symptoms_from_db(connection_string):
    """
    Retrieves symptoms from the SQL Server database's ICD11_Symptoms table.

    Args:
        connection_string: The connection string for the SQL Server database.

    Returns:
        A dictionary where keys are ICD-11 codes and values are lists of
        symptom strings. Returns None on error.
    """
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        query = "SELECT code, symptom_text FROM  llmind.dbo.ICD11_Symptoms"
        cursor.execute(query)
        results = {}
        for row in cursor.fetchall():
            code, symptom_text = row
            if code not in results:
                results[code] = []
            results[code].append(symptom_text)
        conn.close()
        return results
    except pyodbc.Error as e:
        print(f"Error connecting to or querying the database for symptoms: {e}")
        return None

def get_prescriptions_from_db(connection_string):
    """
    Retrieves prescription data from the KGPrime_db table, filtered by disease title.

    Args:
        connection_string:The SQL Server connection string.

    Returns:
        A dictionary where keys are ICD-11 codes and values are lists of
        prescription strings.
    """
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        query = """
                select code,prescription_text
                FROM llmind.dbo.ICD11_Prescriptions
            """
        cursor.execute(query)
        results = {}
        for row in cursor.fetchall():
            disease_code = row.code.strip()  # ICD-11 code is in y_id
            prescription_name = row.prescription_text.strip() # Prescription is in x_name
            if disease_code not in results:
                results[disease_code] = []
            results[disease_code].append(prescription_name)
        conn.close()
        return results
    except pyodbc.Error as e:
        print(f"Error querying the database for prescriptions: {e}")
        return None



def insert_prescriptions_into_db(connection_string, prescriptions_data):
    """
    Inserts prescriptions into the ICD11_Prescriptions table.

    Args:
        connection_string: The SQL Server connection string.
        prescriptions_data: A list of tuples, where each tuple contains (code, prescription_text).
    """
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(
            """
                insert into dbo.ICD11_Prescriptions
                SELECT  code, x_name
                FROM llmind.dbo.KGPrime_db k
                left join llmind.dbo.ICD11_Codes c on k.y_name like '%'+c.title+'%'
                where code is not null
            """
        )
        conn.commit()
        conn.close()
    except pyodbc.Error as e:
        print(f"Error inserting prescriptions into the database: {e}")
        return False  # Indicate failure
    return True


# def build_taxonomy_data(icd11_data: List[Dict]) -> Dict:
#     """
#     Builds a hierarchical taxonomy data structure from the ICD-11 data, using parent-child relationships.

#     Args:
#         icd11_data: A list of dictionaries, where each dictionary represents an ICD-11 entity.

#     Returns:
#         A dictionary representing the taxonomy, ready for D3.js visualization.
#     """
#     # Create a mapping of code to entity for easy access.
#     entity_map = {entity['code']: entity for entity in icd11_data}

#     # Function to recursively build the tree.
#     def build_tree(parent_code=None):
#         nodes = []
#         for entity in icd11_data:
#             code = entity['code']
#             title = entity['title']
            
#             if parent_code is None: # Root
#                 if 'child' in entity:
#                     for child_code_full in entity['child']:
#                          child_code = child_code_full.split('/')[-1]
#                          if child_code in entity_map:
#                             child_entity = entity_map[child_code]
#                             child_node = {
#                                 'name': child_entity['title'],
#                                 'code' : child_entity['code'], # ADDED
#                                 'definition': child_entity['definition'], # ADDED
#                                 'children': build_tree(child_code)
#                             }
#                             nodes.append(child_node)
#             elif parent_code in entity_map and 'child' in entity_map[parent_code]:
                
#                 for child_code_full in entity_map[parent_code]['child']:
                    
#                     child_code = child_code_full.split('/')[-1]
#                     if child_code == code:
#                         node =  {'name': title, 
#                                  'code': code, #ADDED
#                                  'definition': entity['definition'], #ADDED
#                                  'children': []}
#                         if 'child' in entity:
#                             for child_code_full2 in entity['child']:
#                                 child_code2 = child_code_full2.split('/')[-1]
#                                 if child_code2 in entity_map:
#                                     child_entity2 = entity_map[child_code2]
#                                     child_node2 = {
#                                         'name': child_entity2['title'],
#                                         'code':child_entity2['code'], #ADDED
#                                         'definition':child_entity2['definition'], #ADDED
#                                         'children': build_tree(child_code2)
#                                     }
#                                     node['children'].append(child_node2)
#                         nodes.append(node)
#         return nodes

#     # Find the root.  This might require some logic depending on your data structure.
#     #  For this example, I'm assuming there's a top-level category with no parent.
#     root_nodes = build_tree()
    
#     # If there are multiple roots, which is incorrect,  we need to create a single root.
#     if (len(root_nodes)) > 1:
#         root_node = {'name': 'ICD-11 Root', 'children': root_nodes}
#     elif len(root_nodes) == 1:
#         root_node = root_nodes[0]
#     else:
#         root_node = {'name': 'ICD-11 Root', 'children': []}
        

#     return root_node



# def generate_html_tree(taxonomy_data: Dict) -> str:
#     """
#     Generates an HTML file with a D3.js tree visualization of the ICD-11 taxonomy.

#     Args:
#         taxonomy_data: A dictionary representing the taxonomy.

#     Returns:
#         A string containing the HTML code.
#     """
#     html_content = f"""
#     <!DOCTYPE html>
#     <html>
#     <head>
#         <meta charset="utf-8">
#         <title>D3 Tree Visualization</title>
#         <script src="https://d3js.org/d3.v7.min.js"></script>
#         <style>
#             .node circle {{ fill: #fff; stroke: steelblue; stroke-width: 2px; }}
#             .node text {{ font: 12px sans-serif; }}
#             .link {{ fill: none; stroke: #ccc; stroke-width: 1.5px; }}
#         </style>
#     </head>
#     <body>
#         <div id="tree-container"></div>
#         <script>
#             const data = {json.dumps(taxonomy_data)};

#             const width = 7000;
#             const height = 7000;  // Increased height for more vertical space

#             const svg = d3.select("#tree-container")
#                 .append("svg")
#                 .attr("width", width)
#                 .attr("height", height)
#                 .append("g")
#                 .attr("transform", "translate(40,0)");

#             const root = d3.hierarchy(data);
#             // Increased the height parameter (first value) to create more space between levels
#             const treeLayout = d3.tree().size([height - 200, width - 160]);

#             treeLayout(root);

#             // Links
#             svg.selectAll(".link")
#                 .data(root.links())
#                 .enter()
#                 .append("path")
#                 .attr("class", "link")
#                 .attr("d", d3.linkHorizontal()
#                     .x(d => d.y)
#                     .y(d => d.x));

#             // Nodes
#             const node = svg.selectAll(".node")
#                 .data(root.descendants())
#                 .enter()
#                 .append("g")
#                 .attr("class", "node")
#                 .attr("transform", d => `translate(${{d.y}},${{d.x}})`);

#             node.append("circle")
#                 .attr("r", 4.5);

#             node.append("text")
#             .attr("dy", ".31em")
#             .attr("x", d => d.children ? -8 : 8)
#             .style("text-anchor", d => d.children ? "end" : "start")
#             .text(d => d.data.name)
#             .append("title")  // Add title for tooltip
#             .text(d => `Code: ${{d.data.code}}\\nDefinition: ${{d.data.definition}}`);

#         // Add zoom functionality
#         const zoom = d3.zoom()
#             .scaleExtent([0.1, 5])
#             .on("zoom", (event) => {{
#                 svg.attr("transform", event.transform);
#             }});

#         d3.select("svg").call(zoom);
#         </script>
#     </body>
#     </html>
#     """
#     return html_content


def main():
    """
    Main function to:
    1. Retrieve ICD code data starting from the ROOT_URI.
    2. Create the ICD-11 table in the SQL Server database.
    3. Insert the retrieved data into the SQL Server table.
    4. Generate TTL triples from the ICD-11 data extraction.
    """
    results: List[Dict] = []
    # Create the table if it doesn't exist.
    create_table_if_not_exists(SQL_SERVER_CONNECTION_STRING)
    # Using a persistent session for better performance (reuse connections)
    with requests.Session() as session:
        retrieve_code(ROOT_URI, session, results,'',0)



    # Check if ICD11_Codes table exists before inserting data
    try:
        cnxn = pyodbc.connect(SQL_SERVER_CONNECTION_STRING)
        cursor = cnxn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = 'ICD11_Codes'")
        table_exists = cursor.fetchone()
        cursor.close()
        cnxn.close()

        #if not table_exists:
            # Insert the data into the SQL Server table.
        insert_data_into_table(SQL_SERVER_CONNECTION_STRING, results)
        #else:
        #    print("ICD11_Codes table already exists. Skipping data insertion.")

    except Exception as e:
        print(f"Error checking for table: {e}")
        #  Consider more sophisticated error handling (e.g., logging, retrying)
        raise

    # Initialize TTL file with prefixes
    with open(TTL_FILE, "w", encoding="utf-8") as f:
        f.write("@prefix icd: <http://id.who.int/icd/entity/> .\n")
        f.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n")
        f.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\n")
        f.write("@prefix diag: <http://id.who.int/icd/property/> .\n\n")
        f.write("@prefix treat: <http://id.who.int/icd/treatment/> .\n\n")  # New

    try:
        conn = pyodbc.connect(SQL_SERVER_CONNECTION_STRING)
        cursor = conn.cursor()

        # Check if the tables exist
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'llmind' AND TABLE_NAME = 'ICD11_DiagnosticCriteria'")
        table_exists_criteria = cursor.fetchone()

        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'llmind' AND TABLE_NAME = 'ICD11_Symptoms'")
        table_exists_symptoms = cursor.fetchone()
        
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'llmind' AND TABLE_NAME = 'ICD11_Prescriptions'")
        table_exists_prescriptions = cursor.fetchone()
        
        if not table_exists_criteria:
            # Create the table if it doesn't exist
            cursor.execute("""
                CREATE TABLE llmind.dbo.ICD11_DiagnosticCriteria (
                    code NVARCHAR(255) NOT NULL,
                    criterion_type VARCHAR(255) NOT NULL,
                    criterion_text TEXT NOT NULL,
                    FOREIGN KEY (code) REFERENCES llmind.dbo.ICD11_Codes(code)
                )
            """)
            conn.commit()
            print("Table ICD11_DiagnosticCriteria created.")
        else:
            print("Table ICD11_DiagnosticCriteria already exists.")

        if not table_exists_symptoms:
            cursor.execute("""
                CREATE TABLE llmind.dbo.ICD11_Symptoms (
                    code NVARCHAR(255) NOT NULL,
                    symptom_text VARCHAR(255) NOT NULL,
                    FOREIGN KEY (code) REFERENCES llmind.dbo.ICD11_Codes(code)
                )
            """)
            conn.commit()
            print("Table ICD11_Symptoms created.")
        else:
            print("Table ICD11_Symptoms already exists.")

        if not table_exists_prescriptions:
            cursor.execute("""
                CREATE TABLE llmind.dbo.ICD11_Prescriptions (
                    code NVARCHAR(255) NOT NULL,
                    prescription_text VARCHAR(255) NOT NULL,
                    FOREIGN KEY (code) REFERENCES llmind.dbo.ICD11_Codes(code)
                )
            """)
            conn.commit()
            print("Table ICD11_Prescriptions created.")
        else:
            print("Table ICD11_Prescriptions already exists.")
        
        conn.close()
    except pyodbc.Error as e:
        print(f"Error checking or creating database tables: {e}")
        return  # Exit if there's a database error

    # Fetch data from the SQL Server database
    icd11_data = get_icd11_data_from_db(SQL_SERVER_CONNECTION_STRING)


    # Process and insert diagnostic criteria and symptoms
    diagnostic_criteria_to_insert = []
    symptoms_to_insert = []
    prescriptions_to_insert = []
    for entity_data in icd11_data:
        code = entity_data['code']
        diagnostic_criteria_text = entity_data.get('diagnosticCriteria')  # Get the raw text
        definition = entity_data.get('definition')
        longdefinition = entity_data.get('longdefinition')
        
        if diagnostic_criteria_text:
            parsed_criteria = parse_diagnostic_criteria(diagnostic_criteria_text)
            for criterion_type, criterion_text in parsed_criteria:
                diagnostic_criteria_to_insert.append((code, criterion_type, criterion_text))
        
        # Extract and process symptoms
        extracted_symptoms = extract_symptoms(definition, longdefinition)
        for symptom_text in extracted_symptoms:
            symptoms_to_insert.append((code, symptom_text))


    # Insert the parsed diagnostic criteria and symptoms into the database
    if diagnostic_criteria_to_insert:
        insert_diagnostic_criteria_into_db(SQL_SERVER_CONNECTION_STRING, diagnostic_criteria_to_insert)
        print("Diagnostic criteria inserted into the database.")
    else:
        print("No diagnostic criteria found to insert.")

    if symptoms_to_insert:
        insert_symptoms_into_db(SQL_SERVER_CONNECTION_STRING, symptoms_to_insert)
        print("Symptoms inserted into the database.")
    else:
        print("No symptoms found to insert.")

     # Extract and insert prescriptions
    if insert_prescriptions_into_db(SQL_SERVER_CONNECTION_STRING, prescriptions_to_insert):
            print("Prescriptions inserted into the database.")
    else:
            print("Failed to insert prescriptions into the database.")



    # Fetch diagnostic criteria and symptoms *from the database*
    diagnostic_criteria_data = get_diagnostic_criteria_from_db(SQL_SERVER_CONNECTION_STRING)
    symptoms_data = get_symptoms_from_db(SQL_SERVER_CONNECTION_STRING)
    prescriptions_data = get_prescriptions_from_db(SQL_SERVER_CONNECTION_STRING) # Get prescriptions

    if not diagnostic_criteria_data:
        print("Failed to retrieve diagnostic criteria from the database.")
        diagnostic_criteria_data = {}  # Ensure it's an empty dict if there's an error
    
    if not symptoms_data:
        print("Failed to retrieve symptoms from the database.")
        symptoms_data = {}
    
    if not prescriptions_data:
        print("Failed to retrieve prescriptions from the database.")
        prescriptions_data = {}

    # Generate the TTL file
    # ttl_output = generate_ttl(icd11_data, diagnostic_criteria_data, symptoms_data, prescriptions_data)
    # if ttl_output: # Make sure there is something to write
    #     with open(TTL_FILE, "a", encoding="utf-8") as f:
    #         f.write(ttl_output)
    #     print(f"TTL data written to {TTL_FILE}")
    # else:
    #     print("No TTL data to write.")

    # Generate taxonomy data and print it
    # taxonomy_data = build_taxonomy_data(icd11_data)
    # print(json.dumps(taxonomy_data, indent=4))  # Print the taxonomy

    # Generate and save the HTML file
    # html_output = generate_html_tree(taxonomy_data)
    # with open(HTML_FILE, "w", encoding="utf-8") as f:
    #     f.write(html_output)
    # print(f"Taxonomy visualization saved to {HTML_FILE}")



if __name__ == "__main__":
    main()

