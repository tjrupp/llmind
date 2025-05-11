import pyodbc
import re

# Output file for the TTL data
TTL_FILE = "icd11_knowledge_graph.ttl"

# SQL Server connection string
SQL_SERVER_CONNECTION_STRING = "Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=llmind;Uid=sa;Pwd=LLMind2025!;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"

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
        query = "SELECT code, title, definition, longdefinition, inclusions, exclusions, diagnosticCriteria FROM llmind.dbo.ICD11_Codes"
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
    Extracts symptoms from the definition and long definition.
    This is a simplified example and might need more sophisticated NLP techniques.

    Args:
        definition: The definition string.
        longdefinition: The long definition string.

    Returns:
        A list of symptom strings.
    """
    symptoms = []
    # Combine definition and long definition for more comprehensive extraction
    text = f"{definition} {longdefinition}"
    # Simplified symptom extraction using regular expressions (Improve as needed)
    symptom_keywords = [
        "symptoms include", "symptoms are", "characterized by", "manifested by",
        "presents with", "associated with", "signs and symptoms of", "features include"
    ]
    for keyword in symptom_keywords:
        match = re.search(rf"{keyword}\s*([\w\s,()-]+?)(?:\.|$)", text, re.IGNORECASE)
        if match:
            symptom_string = match.group(1).strip()
            # Further split by commas and "and"
            symptoms.extend([s.strip() for s in re.split(r",\s*|and\s*", symptom_string)])
    # Remove empty strings
    symptoms = [s for s in symptoms if s]

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
        "Significant impairment in other important areas of functioning",
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
        "Significant impairment in other important areas of functioning"
    ]
    for symptom in additional_symptoms:
        if re.search(rf"\b{re.escape(symptom)}\b", text, re.IGNORECASE):
            symptoms.append(symptom)

    return list(set(symptoms)) #prevent duplicates



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



def generate_ttl(icd11_data, diagnostic_criteria_data, symptoms_data):
    """
    Generates TTL triples from the ICD-11 data, diagnostic criteria, and symptoms.

    Args:
        icd11_data: A list of dictionaries, where each dictionary represents an ICD-11 entity.
        diagnostic_criteria_data: A dictionary where keys are ICD-11 codes and values are
            lists of dictionaries, each representing a diagnostic criterion.
        symptoms_data: A dictionary where keys are ICD-11 codes and values are lists of
            symptom strings.

    Returns:
        A string containing the TTL triples. Returns an empty string if data is None or empty.
    """
    if not icd11_data:
        return ""

    ttl_triples = []

    for entity_data in icd11_data:
        entity_id = entity_data.get("code")
        if not entity_id:
            print(f"Entity has no code: {entity_data}")
            continue  # Skip this entity

        entity_uri = f"icd:{entity_id}"

        # Add type information.  For simplicity, assume all are icd:Disease.
        ttl_triples.append(f"<{entity_uri}> rdf:type icd:Disease .")

        title = entity_data.get("title")
        if title:
            escaped_title = title.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
            ttl_triples.append(f"<{entity_uri}> icd:title \"{escaped_title}\" .")

        definition = entity_data.get("definition")
        if definition:
            escaped_definition = definition.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
            ttl_triples.append(f"<{entity_uri}> icd:definition \"{escaped_definition}\" .")
        
        longdefinition = entity_data.get("longdefinition")
        if longdefinition:
            escaped_longdefinition = longdefinition.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
            ttl_triples.append(f"<{entity_uri}> icd:longdefinition \"{escaped_longdefinition}\" .")

        # Inclusions and exclusions are lists in the database, handle them
        inclusions = entity_data.get("inclusions")
        if inclusions:
            escaped_inclusions = inclusions.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
            ttl_triples.append(f"<{entity_uri}> icd:inclusions \"{escaped_inclusions}\" .")

        exclusions = entity_data.get("exclusions")
        if exclusions:
            escaped_exclusions = exclusions.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
            ttl_triples.append(f"<{entity_uri}> icd:exclusions \"{escaped_exclusions}\" .")

        # Handle diagnostic criteria from the separate table
        if entity_id in diagnostic_criteria_data:
            for criterion in diagnostic_criteria_data[entity_id]:
                criterion_type = criterion['type']
                criterion_text = criterion['text']
                escaped_text = criterion_text.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
                ttl_triples.append(f"<{entity_uri}> icd:hasCriterion [ rdf:type icd:DiagnosticCriterion ;"
                                   f" icd:criterionType \"{criterion_type}\" ;"
                                   f" icd:criterionText \"{escaped_text}\" ] .")
        
        # Handle symptoms
        if entity_id in symptoms_data:
            for symptom in symptoms_data[entity_id]:
                escaped_symptom = symptom.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
                ttl_triples.append(f"<{entity_uri}> icd:hasSymptom \"{escaped_symptom}\" .")

        #  The SQL table doesn't have parent/child, so we can't represent the hierarchy.
        #  If your SQL Server table *does* have parent/child relationships, you'll need to
        #  modify the SQL query and this function to handle that.

    return "\n".join(ttl_triples)

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
            results[code].append({'type': criterion_type, 'text': criterion_text})
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
        query = "SELECT code, symptom_text FROM llmind.dbo.ICD11_Symptoms"
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



def main():
    """
    Main function to drive the data extraction and TTL generation.
    """
    # Initialize TTL file with prefixes
    with open(TTL_FILE, "w", encoding="utf-8") as f:
        f.write("@prefix icd: <http://id.who.int/icd/entity/> .\n")
        f.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n")
        f.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\n")
        f.write("@prefix icd:DiagnosticCriterion <http://id.who.int/icd/property/diagnosticCriterion> .\n\n") # Define the new class
        f.write("@prefix icd:hasSymptom <http://id.who.int/icd/property/hasSymptom> .\n\n")  # New

    try:
        conn = pyodbc.connect(SQL_SERVER_CONNECTION_STRING)
        cursor = conn.cursor()

        # Check if the tables exist
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'llmind' AND TABLE_NAME = 'ICD11_DiagnosticCriteria'")
        table_exists_criteria = cursor.fetchone()

        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'llmind' AND TABLE_NAME = 'ICD11_Symptoms'")
        table_exists_symptoms = cursor.fetchone()
        
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
        
        conn.close()
    except pyodbc.Error as e:
        print(f"Error checking or creating database tables: {e}")
        return  # Exit if there's a database error

    # Fetch data from the SQL Server database
    icd11_data = get_icd11_data_from_db(SQL_SERVER_CONNECTION_STRING)


    # Process and insert diagnostic criteria and symptoms
    diagnostic_criteria_to_insert = []
    symptoms_to_insert = []
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

    # Fetch diagnostic criteria and symptoms *from the database*
    diagnostic_criteria_data = get_diagnostic_criteria_from_db(SQL_SERVER_CONNECTION_STRING)
    symptoms_data = get_symptoms_from_db(SQL_SERVER_CONNECTION_STRING)
    
    if not diagnostic_criteria_data:
        print("Failed to retrieve diagnostic criteria from the database.")
        diagnostic_criteria_data = {}  # Ensure it's an empty dict if there's an error
    
    if not symptoms_data:
        print("Failed to retrieve symptoms from the database.")
        symptoms_data = {}

    # Generate the TTL file
    ttl_output = generate_ttl(icd11_data, diagnostic_criteria_data, symptoms_data)
    if ttl_output: # Make sure there is something to write
        with open(TTL_FILE, "a", encoding="utf-8") as f:
            f.write(ttl_output)
        print(f"TTL data written to {TTL_FILE}")
    else:
        print("No TTL data to write.")



if __name__ == "__main__":
    main()

