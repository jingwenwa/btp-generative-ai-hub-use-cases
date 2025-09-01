import os
import configparser
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from hana_ml import dataframe

# ---------------------------------------------
# Step 0: Setup Connection (Cloud Foundry vs Local)
# ---------------------------------------------

if 'VCAP_APPLICATION' in os.environ:
    from app.utilities_hana import kmeans_and_tsne
    hanaURL = os.getenv('DB_ADDRESS')
    hanaPort = os.getenv('DB_PORT')
    hanaUser = os.getenv('DB_USER')
    hanaPW = os.getenv('DB_PASSWORD')
else:
    from utilities_hana import kmeans_and_tsne
    config = configparser.ConfigParser()
    config.read('../config.ini')
    hanaURL = config['database']['address']
    hanaPort = config['database']['port']
    hanaUser = config['database']['user']
    hanaPW = config['database']['password']

# Step 1: Establish a connection to SAP HANA
connection = dataframe.ConnectionContext(hanaURL, hanaPort, hanaUser, hanaPW)

app = Flask(__name__)
CORS(app)

# ---------------------------------------------
# Step 1: Category Tables and API
# ---------------------------------------------

def create_categories_table_if_not_exists():
    create_table_sql = """
    DO BEGIN
        DECLARE table_exists INT;
        SELECT COUNT(*) INTO table_exists
        FROM SYS.TABLES 
        WHERE TABLE_NAME = 'CATEGORIES' AND SCHEMA_NAME = CURRENT_SCHEMA;
        IF table_exists = 0 THEN
            CREATE TABLE CATEGORIES (
                "index" INTEGER,
                "category_label" NVARCHAR(100),
                "category_descr" NVARCHAR(5000),
                "category_embedding" REAL_VECTOR 
                    GENERATED ALWAYS AS VECTOR_EMBEDDING("CATEGORY_DESCR", 'DOCUMENT', 'SAP_NEB.20240715')
            );
        END IF;
    END;
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
                PROJECT_ID INT,
                CATEGORY_ID INT
            );
        END IF;
    END;
    """
    cursor = connection.connection.cursor()
    cursor.execute(create_table_sql)
    cursor.close()  

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
    
    for index, (title, description) in enumerate(categories.items()):
        insert_sql = f"""
            INSERT INTO CATEGORIES ("INDEX", "CATEGORY_LABEL", "CATEGORY_DESCR")
            VALUES ({index}, '{title.replace("'", "''")}', '{description.replace("'", "''")}')
        """
        cursor.execute(insert_sql)
    
    categories_df = dataframe.DataFrame(connection, 'SELECT * FROM DBUSER.CATEGORIES')
    advisories_df = dataframe.DataFrame(connection, 'SELECT "project_number", "topic" FROM DBUSER.ADVISORIES4')
    
    for advisory in advisories_df.collect().to_dict(orient='records'):
        project_number = advisory['project_number']
        topic = advisory['topic']
        if not isinstance(project_number, int):
            continue
    
        similarities = []
        for category in categories_df.collect().to_dict(orient='records'):
            category_id = category['index']
            category_description = category['CATEGORY_DESCR']
            
            # Use precomputed embeddings
            similarity_sql = f"""
                SELECT COSINE_SIMILARITY(
                    (SELECT SOLUTION_EMBEDDING FROM DBUSER.KNOWLEDGE_BASE WHERE TOPIC = '{topic.replace("'", "''")}' LIMIT 1),
                    (SELECT CATEGORY_EMBEDDING FROM DBUSER.CATEGORIES WHERE TOPIC = '{CATEGORY_DESCR.replace("'", "''")}' LIMIT 1)
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
                VALUES ('{project_number}', {category_id})
            """
            cursor.execute(insert_sql)
    
    cursor.close()
    return jsonify({"message": "Categories and project categories updated successfully"}), 200

@app.route('/get_all_project_categories', methods=['GET'])
def get_all_project_categories():
    sql_query = """
        SELECT pbc."PROJECT_ID", c."CATEGORY_LABEL"
        FROM "PROJECT_BY_CATEGORY" pbc
        JOIN "CATEGORIES" c ON pbc."CATEGORY_ID" = c."CATEGORY_ID"
    """
    hana_df = dataframe.DataFrame(connection, sql_query)
    results = hana_df.collect().to_dict(orient='records')
    return jsonify({"project_categories": results}), 200

@app.route('/get_categories', methods=['GET'])
def get_categories():
    sql_query = 'SELECT "CATEGORY_ID", "CATEGORY_LABEL", "CATEGORY_DESCR" FROM "CATEGORIES"'
    hana_df = dataframe.DataFrame(connection, sql_query)
    results = hana_df.collect().to_dict(orient='records')
    return jsonify(results), 200

# ---------------------------------------------
# Knowledge Base / Embedding APIs
# ---------------------------------------------

@app.route('/compare_topic', methods=['POST'])
def compare_topic():
    data = request.get_json()
    query_topic = data.get('topic')
    if not query_topic:
        return jsonify({"error": "Topic is required"}), 400

    # Use precomputed embeddings
    sql_query = f"""
            SELECT TOP 5 
                kb.ID, 
                kb.TOPIC, 
                kb.SOLUTION,
                COSINE_SIMILARITY(
                    kb.SOLUTION_EMBEDDING,
                    (SELECT TOP 1 SOLUTION_EMBEDDING 
                    FROM DBUSER.KNOWLEDGE_BASE 
                    WHERE TOPIC = '{query_topic.replace("'", "''")}')
    """
    df = dataframe.DataFrame(connection, sql_query)
    results = df.collect().to_dict(orient='records')
    return jsonify({"results": results}), 200

@app.route('/knowledge_base', methods=['GET'])
def get_knowledge_base():
    sql_query = 'SELECT * FROM DBUSER.KNOWLEDGE_BASE'
    df = dataframe.DataFrame(connection, sql_query)
    results = df.collect().to_dict(orient='records')
    return jsonify({"knowledge_base": results}), 200

# ---------------------------------------------
# Clustering APIs (keep original logic)
# ---------------------------------------------
# ... include your original clustering, refresh_clusters, get_clusters, get_clusters_description code here
# but replace any VECTOR_EMBEDDING calls with precomputed embeddings if needed

# ---------------------------------------------
# Root Health Check
# ---------------------------------------------
@app.route('/', methods=['GET'])
def root():
    return 'Knowledge Base API: Health Check Successful.', 200

def create_app():
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)