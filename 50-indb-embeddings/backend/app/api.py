import math
import pandas as pd
import numpy as np
import re

import os
import configparser
from pathlib import Path

from datetime import datetime
from flask import Flask, request, jsonify, json, Response
from flask_cors import CORS
from hana_ml import dataframe
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from sql_formatter.core import format_sql
from gen_ai_hub.proxy.langchain.openai import ChatOpenAI
from gen_ai_hub.proxy.core.proxy_clients import get_proxy_client

def nan_to_null(df):
    """Convert all NaN or NaT in a DataFrame to None for JSON serialization."""
    return df.replace({np.nan: None, pd.NaT: None})

# Check if the application is running on Cloud Foundry
if 'VCAP_APPLICATION' in os.environ:
    from app.utilities_hana import kmeans_and_tsne  # works in CF
    
    # Running on Cloud Foundry, use environment variables
    hanaURL = os.getenv('DB_ADDRESS')
    hanaPort = os.getenv('DB_PORT')
    hanaUser = os.getenv('DB_USER')
    hanaPW = os.getenv('DB_PASSWORD')
else:
    from utilities_hana import kmeans_and_tsne  # works in local machine
    
    # Not running on Cloud Foundry, read from config.ini file
    config = configparser.ConfigParser()
    config.read('config.ini')
    hanaURL = config['database']['address']
    hanaPort = config['database']['port']
    hanaUser = config['database']['user']
    hanaPW = config['database']['password']

# Step 1: Establish a connection to SAP HANA
connection = dataframe.ConnectionContext(hanaURL, hanaPort, hanaUser, hanaPW)

# Initialize LLM once
proxy_client = get_proxy_client('gen-ai-hub')
llm = ChatOpenAI(proxy_model_name='gpt-5', temperature=0, proxy_client=proxy_client)

app = Flask(__name__)
CORS(app)

# -------------------------------
# Table creation utilities
# -------------------------------
def create_categories_table_if_not_exists():
    create_table_sql = """
        DO BEGIN
            DECLARE table_exists INT;
            
            -- Check and create CATEGORIES table
            SELECT COUNT(*) INTO table_exists
            FROM SYS.TABLES 
            WHERE TABLE_NAME = 'CATEGORIES' AND SCHEMA_NAME = CURRENT_SCHEMA;
            
            IF table_exists = 0 THEN
                CREATE TABLE CATEGORIES (
                    "index" INTEGER,
                    "category_label" NVARCHAR(100),
                    "category_descr" NVARCHAR(5000),
                    "category_embedding" REAL_VECTOR 
                        GENERATED ALWAYS AS VECTOR_EMBEDDING("category_descr", 'DOCUMENT', 'SAP_NEB.20240715')
                );
            END IF;
        END
    """
    cursor = connection.connection.cursor()
    cursor.execute(create_table_sql)
    cursor.close()

def create_project_by_category_table_if_not_exists():
    create_table_sql = """
        DO BEGIN
            DECLARE table_exists INT;
            
            SELECT COUNT(*) INTO table_exists
            FROM SYS.TABLES 
            WHERE TABLE_NAME = 'PROJECT_BY_CATEGORY' AND SCHEMA_NAME = CURRENT_SCHEMA;
            
            IF table_exists = 0 THEN
                CREATE TABLE PROJECT_BY_CATEGORY (
                    PROJECT_ID NVARCHAR(255),
                    CATEGORY_ID INT
                );
            END IF;
        END
    """
    cursor = connection.connection.cursor()
    cursor.execute(create_table_sql)
    cursor.close()  

def create_clustering_table_if_not_exists():
    create_table_sql = """
        DO BEGIN
            DECLARE table_exists INT;
            
            -- CLUSTERING table
            SELECT COUNT(*) INTO table_exists
            FROM SYS.TABLES 
            WHERE TABLE_NAME = 'CLUSTERING' AND SCHEMA_NAME = CURRENT_SCHEMA;
            
            IF table_exists = 0 THEN
                CREATE TABLE CLUSTERING (
                    PROJECT_NUMBER NVARCHAR(255),
                    x DOUBLE,
                    y DOUBLE,
                    CLUSTER_ID INT
                );
            END IF;
            
            -- CLUSTERING_DATA table
            SELECT COUNT(*) INTO table_exists
            FROM SYS.TABLES 
            WHERE TABLE_NAME = 'CLUSTERING_DATA' AND SCHEMA_NAME = CURRENT_SCHEMA;
            
            IF table_exists = 0 THEN
                CREATE TABLE CLUSTERING_DATA (
                    CLUSTER_ID INT,
                    CLUSTER_DESCRIPTION NVARCHAR(255),
                    EMBEDDING REAL_VECTOR GENERATED ALWAYS AS VECTOR_EMBEDDING(CLUSTER_DESCRIPTION, 'DOCUMENT', 'SAP_NEB.20240715')
                );
            END IF;
        END
    """
    cursor = connection.connection.cursor()
    cursor.execute(create_table_sql)
    cursor.close()  

def create_table_if_not_exists(schema_name, table_name):
    create_table_sql = f"""
        DO BEGIN
            DECLARE table_exists INT;
            SELECT COUNT(*) INTO table_exists
            FROM SYS.TABLES 
            WHERE TABLE_NAME = '{table_name.upper()}' AND SCHEMA_NAME = '{schema_name.upper()}';
            
            IF table_exists = 0 THEN
                CREATE TABLE {schema_name}.{table_name} (
                    TEXT_ID INT GENERATED BY DEFAULT AS IDENTITY,
                    TEXT NVARCHAR(5000),
                    EMBEDDING REAL_VECTOR GENERATED ALWAYS AS VECTOR_EMBEDDING(TEXT, 'DOCUMENT', 'SAP_NEB.20240715')
                );
            END IF;
        END
    """
    cursor = connection.connection.cursor()
    cursor.execute(create_table_sql)
    cursor.close()  

# -------------------------------
# Flask Endpoints
# -------------------------------
@app.route('/update_categories_and_projects', methods=['POST'])
def update_categories_and_projects():
    data = request.get_json()
    categories = data
    
    if not categories:
        return jsonify({"error": "No categories provided"}), 400
    
    cursor = connection.connection.cursor()
    
    create_categories_table_if_not_exists()
    cursor.execute("TRUNCATE TABLE CATEGORIES")
    
    create_project_by_category_table_if_not_exists()
    cursor.execute("TRUNCATE TABLE PROJECT_BY_CATEGORY")
    
    # Insert categories
    for index, (title, description) in enumerate(categories.items()):
        insert_sql = f"""
            INSERT INTO CATEGORIES ("index", "category_label", "category_descr")
            VALUES ({index}, '{title.replace("'", "''")}', '{description.replace("'", "''")}')
        """
        cursor.execute(insert_sql)
    
    categories_df = dataframe.DataFrame(connection, 'SELECT * FROM CATEGORIES')
    advisories_df = dataframe.DataFrame(connection, 'SELECT "RULE_ID", "TOPIC" FROM MHA_ADVISORIES4')
    
    # Match advisories to categories using COSINE similarity
    for advisory in advisories_df.collect().to_dict(orient='records'):
        rule_id = advisory['RULE_ID']
        topic = advisory['TOPIC']
        
        if not isinstance(rule_id, int) and not (isinstance(rule_id, str) and rule_id.isdigit()):
            continue
        
        similarities = []
        for category in categories_df.collect().to_dict(orient='records'):
            category_id = category['index']
            category_description = category['category_descr']
            
            similarity_sql = f"""
                SELECT COSINE_SIMILARITY(
                    VECTOR_EMBEDDING('{topic.replace("'", "''")}', 'DOCUMENT', 'SAP_NEB.20240715'),
                    VECTOR_EMBEDDING('{category_description.replace("'", "''")}', 'DOCUMENT', 'SAP_NEB.20240715')
                ) AS similarity
                FROM DUMMY
            """
            similarity_df = dataframe.DataFrame(connection, similarity_sql)
            similarity_results = similarity_df.collect()
            if not similarity_results.empty:
                similarity = similarity_results.iloc[0]['SIMILARITY']
                similarities.append((category_id, similarity))
        
        if similarities:
            most_similar_category = max(similarities, key=lambda x: x[1])
            category_id = most_similar_category[0]
            insert_sql = f"""
                INSERT INTO PROJECT_BY_CATEGORY ("PROJECT_ID", "CATEGORY_ID")
                VALUES ('{rule_id}', {category_id})
            """
            cursor.execute(insert_sql)
    
    cursor.close()
    return jsonify({"message": "Categories and project categories updated successfully"}), 200

@app.route('/get_all_project_categories', methods=['GET'])
def get_all_project_categories():
    sql_query = """
        SELECT pbc."PROJECT_ID", c."category_label"
        FROM "PROJECT_BY_CATEGORY" pbc
        JOIN "CATEGORIES" c ON pbc."CATEGORY_ID" = c."index"
    """
    hana_df = dataframe.DataFrame(connection, sql_query)
    project_categories = hana_df.collect()
    results = project_categories.to_dict(orient='records')
    return jsonify({"project_categories": results}), 200

@app.route('/get_categories', methods=['GET'])
def get_categories():
    sql_query = 'SELECT "index", "category_label", "category_descr" FROM "CATEGORIES"'
    hana_df = dataframe.DataFrame(connection, sql_query)
    categories = hana_df.collect()
    results = categories.to_dict(orient='records')
    return jsonify(results), 200

@app.route('/get_advisories_by_expert_and_category', methods=['GET'])
def get_advisories_by_expert_and_category():
    expert = request.args.get('expert')
    
    if not expert:
        return jsonify({"error": "Expert is required"}), 400
    
    sql_query = f"""
        SELECT c."category_label" AS category, COUNT(a."RULE_ID") AS projects
        FROM "PROJECT_BY_CATEGORY" pbc
        JOIN "CATEGORIES" c ON pbc."CATEGORY_ID" = c."index"
        JOIN "MHA_ADVISORIES4" a ON pbc."PROJECT_ID" = a."RULE_ID"
        WHERE a."NSMAN_ID" = '{expert.replace("'", "''")}'
        GROUP BY c."category_label"
    """
    hana_df = dataframe.DataFrame(connection, sql_query)
    advisories_by_category = hana_df.collect()
    results = advisories_by_category.to_dict(orient='records')
    return jsonify({"advisories_by_category": results}), 200

@app.route('/compare_text_to_existing', methods=['POST'])
def compare_text_to_existing():
    try:
        data = request.get_json()
        schema_name = data.get('schema_name', 'DBUSER')
        query_text = data.get('query_text', '')

        if not query_text:
            return jsonify({"error": "query_text is required"}), 400

        similarities = []

        # --- Use LM to extract NSMAN_ID, LOCATION_NAME, SLOT_DATE ---
        prompt_template = PromptTemplate(
            input_variables=["query_text"],
            template="""
                    You are an AI assistant for extracting booking information from user queries.

                    From the following query, extract:
                    - NSMAN_ID (numeric string)
                    - LOCATION_NAME (or null if not mentioned)
                    - SLOT_DATE in YYYY-MM-DD format (or null if not mentioned)

                    Return strictly JSON in this format:

                    {{
                    "nsman_id": "...",
                    "location_name": "...",
                    "slot_date": "..."
                    }}

                    User query: "{query_text}"
                    """
        )

        chain = prompt_template | llm
        response = chain.invoke({"query_text": query_text})

        try:
            extraction = json.loads(response.content)
            nsman_id = extraction.get("nsman_id")
            location_name = extraction.get("location_name")
            slot_date = extraction.get("slot_date")
        except Exception:
            return jsonify({"error": "Failed to parse LM extraction"}), 400

        if not nsman_id:
            return jsonify({"error": "NSMAN_ID not found"}), 400

        # --- Case 1: If location_name detected, query BOOKINGS_AVAILABILITY ---
        if location_name:
            slot_query = f"""
                SELECT "LOCATION_NAME", "SLOT_DATE", "SLOT_TIME"
                FROM {schema_name}.BOOKINGS_AVAILABILITY
                WHERE UPPER("LOCATION_NAME") = UPPER('{location_name}')
            """
            if slot_date:
                slot_query += f""" AND "SLOT_DATE" = '{slot_date}' """
            slot_query += """
                ORDER BY "SLOT_DATE" DESC, "SLOT_TIME" DESC
                LIMIT 3
            """
            slot_df = dataframe.DataFrame(connection, slot_query)
            slots = slot_df.collect().to_dict(orient='records')

            solution_vals = [
                f"{slot['LOCATION_NAME']} | {slot['SLOT_DATE']} | {slot['SLOT_TIME']}" 
                for slot in slots
            ]

            similarities.append({
                "NSMAN_ID": nsman_id,
                "SOLUTION": solution_vals[0] if len(solution_vals) > 0 else None,
                "SOLUTION_TWO": solution_vals[1] if len(solution_vals) > 1 else None,
                "SOLUTION_THREE": solution_vals[2] if len(solution_vals) > 2 else None,
            })

        # --- Case 2: No location_name → fallback to advisory table ---
        else:
            sql_query = f"""
                SELECT "SOLUTION",
                       "SOLUTION_TWO",
                       "SOLUTION_THREE"
                FROM {schema_name}.MHA_ADVISORIES4
                WHERE "NSMAN_ID" = '{nsman_id}'
                LIMIT 3
            """
            hana_df = dataframe.DataFrame(connection, sql_query)
            solutions = hana_df.collect().to_dict(orient='records')
            for sol in solutions:
                similarities.append({
                    "NSMAN_ID": nsman_id,
                    "SOLUTION": sol.get("SOLUTION"),
                    "SOLUTION_TWO": sol.get("SOLUTION_TWO"),
                    "SOLUTION_THREE": sol.get("SOLUTION_THREE")
                })

        return jsonify({"similarities": similarities}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/get_ippt_history', methods=['POST'])
def get_ippt_history():
    schema_name = request.args.get('schema_name', 'DBUSER')   # default schema

    # Get NSMAN_ID from request body
    data = request.get_json(silent=True) or {}
    nsman_id = data.get("NSMAN_ID")

    if not nsman_id:
        return jsonify({"error": "NSMAN_ID is required in request body"}), 400

    # Build SQL query safely
    sql_query = f'SELECT * FROM {schema_name}.MHA_IPPT_HISTORY'
    if nsman_id.isdigit():
        sql_query += f' WHERE "NSMAN_ID" = {nsman_id}'
    else:
        sql_query += f' WHERE "NSMAN_ID" = \'{nsman_id}\''

    hana_df = dataframe.DataFrame(connection, sql_query)
    ippt_history_df = hana_df.collect()

    # Convert NaN → None for JSON
    ippt_history_df = nan_to_null(ippt_history_df)

    results = ippt_history_df.to_dict(orient='records')
    return jsonify({"ippt_history": results}), 200

@app.route('/get_nsman_mapped', methods=['POST'])
def get_nsman_mapped():
    schema_name = request.args.get('schema_name', 'DBUSER')
    data = request.get_json(silent=True) or {}

    nsman_id = data.get("NSMAN_ID")
    if not nsman_id:
        return jsonify({"error": "NSMAN_ID is required in request body"}), 400

    sql_query = f"""
        SELECT 
            "NSMAN_ID",
            "NAME",
            "EMAIL",
            "PES_STATUS_ID",
            "RANK_CODE",
            "MEANING",
            "BIRTHDAY",
            "CURRENT_GRADE"
        FROM {schema_name}.MHA_NSMAN_MAPPED
        WHERE "NSMAN_ID" = '{nsman_id}'
    """
    hana_df = dataframe.DataFrame(connection, sql_query)
    nsman_df = hana_df.collect()

    # Convert to JSON-friendly format
    results = []
    for row in nsman_df.to_dict(orient='records'):
        results.append({
            "NS ID": row["NSMAN_ID"],
            "Name": row["NAME"],
            "Email": row["EMAIL"],
            "PES Status": row["PES_STATUS_ID"],
            "Rank Code": row["RANK_CODE"],
            "Birthday": row["BIRTHDAY"],
            "Current Grade": row["CURRENT_GRADE"]
        })

    return jsonify({"nsman_mapped": results}), 200


@app.route('/get_project_details', methods=['GET'])
def get_project_details():
    schema_name = request.args.get('schema_name', 'DBUSER')
    project_number = request.args.get('project_number')
    
    if not project_number:
        return jsonify({"error": "Project number is required"}), 400
    
    # SQL query to join ADVISORIES and COMMENTS tables on project_number
    sql_query = f"""
        SELECT a."architect", a."index" AS advisories_index, a."pcb_number", a."project_date", 
               a."project_number", a."solution", a."topic",
               c."comment", c."comment_date", c."index" AS comments_index
        FROM {schema_name}.advisories4 a
        LEFT JOIN {schema_name}.COMMENTS4 c
        ON a."project_number" = c."project_number"
        WHERE a."project_number" = {project_number}
    """
    hana_df = dataframe.DataFrame(connection, sql_query)
    project_details = hana_df.collect()  # Return results as a pandas DataFrame

    # Convert results to a list of dictionaries for JSON response
    results = project_details.to_dict(orient='records')
    return jsonify({"project_details": results}), 200


@app.route('/get_all_projects', methods=['GET'])
def get_all_projects():
    schema_name = request.args.get('schema_name', 'DBUSER')
    
    sql_query = f"""
        SELECT * FROM (
            SELECT a."architect", a."index" AS advisories_index, a."pcb_number", a."project_date", 
                   a."project_number", a."solution", a."topic",
                   c."comment", c."comment_date", c."index" AS comments_index,
                   ROW_NUMBER() OVER (PARTITION BY a."project_number" ORDER BY a."index") AS row_num
            FROM {schema_name}.advisories4 a
            LEFT JOIN {schema_name}.COMMENTS4 c
            ON a."project_number" = c."project_number"
        ) subquery
        WHERE row_num = 1
    """
    hana_df = dataframe.DataFrame(connection, sql_query)
    
    all_projects_df = hana_df.collect()
    all_projects_df = nan_to_null(all_projects_df)  # <-- ensures no NaN

    results = all_projects_df.to_dict(orient='records')
    return jsonify({"all_projects": results}), 200

# -------------------------------
# Root Health Check
# -------------------------------
@app.route('/', methods=['GET'])
def root():
    return 'Embeddings Base API: Health Check Successful.', 200

def create_app():
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port, debug=False)
