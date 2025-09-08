import os
import configparser
from pathlib import Path
from flask import Flask, request, jsonify, json, Response
from flask_cors import CORS
from hana_ml import dataframe
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from sql_formatter.core import format_sql
from gen_ai_hub.proxy.langchain.openai import ChatOpenAI
from gen_ai_hub.proxy.core.proxy_clients import get_proxy_client

# Check if the application is running on Cloud Foundry
if 'VCAP_APPLICATION' in os.environ:
    hanaURL = os.getenv('DB_ADDRESS')
    hanaPort = os.getenv('DB_PORT')
    hanaUser = os.getenv('DB_USER')
    hanaPW = os.getenv('DB_PASSWORD')
else:
    BASE_DIR = Path(__file__).resolve().parent
    config_path = BASE_DIR / 'config.ini'
    config = configparser.ConfigParser()
    read_files = config.read(config_path)
    if not read_files:
        raise FileNotFoundError(f"Could not find config file at {config_path}")
    if 'database' not in config:
        raise KeyError("Missing 'database' section in config.ini")
    hanaURL = config['database'].get('address')
    hanaPort = config['database'].get('port')
    hanaUser = config['database'].get('user')
    hanaPW = config['database'].get('password')

# SAP HANA connection
connection = dataframe.ConnectionContext(hanaURL, hanaPort, hanaUser, hanaPW)

# LLM initialization
proxy_client = get_proxy_client('gen-ai-hub')
llm = ChatOpenAI(proxy_model_name='gpt-5', temperature=0, proxy_client=proxy_client)

app = Flask(__name__)
CORS(app)

# New RDF graph URIs
NEW_GRAPH = "http://www.semanticweb.org/ontologies/2025/advisory-rdf-test"
NEW_GRAPH_INFERRED = "http://www.semanticweb.org/ontologies/2025/advisory-inferred-triples"

@app.route('/execute_query_raw', methods=['POST'])
def execute_query_raw():
    try:
        query = request.data.decode('utf-8')
        query_type = request.args.get('query_type', 'sparql')
        response_format = request.args.get('format', 'json')
        if not query:
            return jsonify({'error': 'Query is required'}), 400
        cursor = connection.connection.cursor()
        if query_type == 'sparql':
            mimetype = 'application/sparql-results+csv' if response_format == 'csv' else 'application/sparql-results+json'
            result = cursor.callproc('SPARQL_EXECUTE', (query, mimetype, '?', '?'))
            return Response(result[2], mimetype='text/csv' if response_format=='csv' else 'application/json') if response_format=='csv' else jsonify(json.loads(result[2]))
        elif query_type == 'sql':
            cursor.execute(query)
            rows = cursor.fetchall()
            headers = [desc[0] for desc in cursor.description]
            if response_format=='csv':
                csv_data = ','.join(headers) + '\n'
                csv_data += '\n'.join([','.join(map(str,row)) for row in rows])
                return Response(csv_data, mimetype='text/csv')
            else:
                return jsonify([dict(zip(headers,row)) for row in rows])
        else:
            return jsonify({'error': 'Invalid query_type. Use "sparql" or "sql".'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/execute_sparql_query', methods=['GET'])
def execute_sparql_query():
    try:
        query = request.args.get('query')
        response_format = request.args.get('format', 'json')
        if not query:
            return jsonify({'error': 'Query is required'}), 400
        cursor = connection.connection.cursor()
        mimetype = 'application/sparql-results+csv' if response_format == 'csv' else 'application/sparql-results+json'
        result = cursor.callproc('SPARQL_EXECUTE', (query, mimetype, '?', '?'))
        return Response(result[2], mimetype='text/csv') if response_format=='csv' else jsonify(json.loads(result[2]))
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/translate_nl_to_sparql', methods=['POST'])
def translate_nl_to_sparql():
    try:
        data = request.get_json()
        nl_query = data.get('nl_query')
        if not nl_query:
            return jsonify({'error': 'Natural language query required'}), 400

        cursor = connection.connection.cursor()
        cursor.execute("SELECT ONTOLOGY_QUERY, PROPERTY_QUERY, CLASSES_QUERY, INSTRUCTIONS, PREFIXES, GRAPH, GRAPH_INFERRED, QUERY_EXAMPLE, TEMPLATE FROM ONTOLOGY_CONFIG")
        config = cursor.fetchone()
        ontology_query, property_query, classes_query, instructions, prefixes, _, _, query_example, template_config = config

        # Override graph URIs
        graph = NEW_GRAPH
        graph_inferred = NEW_GRAPH_INFERRED

        # GET ONTOLOGY
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (ontology_query, 'application/sparql-results+csv', '?', '?'))
        ontology = result[2]

        # GET PROPERTIES
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (property_query, 'application/sparql-results+json', '?', '?'))
        properties = result[2]

        # GET CLASSES
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (classes_query, 'application/sparql-results+json', '?', '?'))
        classes = result[0]

        prompt_template = PromptTemplate(
            input_variables=["nl_query", "classes", "properties", "ontology", "graph", "graph_inferred", "prefixes", "query_example", "instructions"],
            template=template_config
        )

        chain = prompt_template | llm
        response = chain.invoke({
            "nl_query": nl_query,
            "classes": classes,
            "properties": properties,
            "ontology": ontology,
            "graph": graph,
            "graph_inferred": graph_inferred,
            "prefixes": prefixes,
            "query_example": query_example,
            "instructions": instructions
        })

        sparql_query = response.content.strip()
        return jsonify({'sparql_query': sparql_query}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/translate_nl_to_new', methods=['POST'])
def translate_nl_to_new():
    try:
        data = request.get_json()
        nl_query = data.get('nl_query')
        if not nl_query:
            return jsonify({'error': 'Natural language query required'}), 400

        cursor = connection.connection.cursor()
        cursor.execute("""
            SELECT ONTOLOGY_QUERY, PROPERTY_QUERY, CLASSES_QUERY, INSTRUCTIONS, PREFIXES, GRAPH, GRAPH_INFERRED, 
                   QUERY_EXAMPLE, TEMPLATE, TEMPLATE_SIMILARITY, QUERY_TEMPLATE, QUERY_TEMPLATE_NO_TOPIC 
            FROM ONTOLOGY_CONFIG
        """)
        config = cursor.fetchone()
        ontology_query, property_query, classes_query, instructions, prefixes, _, _, query_example, template, template_similarity, query_template, query_template_no_topic = config

        # Override graph URIs
        graph = NEW_GRAPH
        graph_inferred = NEW_GRAPH_INFERRED

        # GET ONTOLOGY
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (ontology_query, 'application/sparql-results+csv', '?', '?'))
        ontology = result[2]

        # GET PROPERTIES
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (property_query, 'application/sparql-results+json', '?', '?'))
        properties = result[2]

        # GET CLASSES
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (classes_query, 'application/sparql-results+json', '?', '?'))
        classes = result[0]

        # Topic extraction
        prompt_template_topic = PromptTemplate(input_variables=["question"], template=template_similarity)
        chain_topic = prompt_template_topic | llm | StrOutputParser()
        response_topic = chain_topic.invoke({'question': nl_query})
        response_topic = response_topic.strip('```python\n').strip('\n```')
        response_topic = json.loads(response_topic)
        topic = response_topic["topic"]
        query = response_topic["query"]

        # SPARQL generation
        prompt_template_sparql = PromptTemplate(
            input_variables=["nl_query", "classes", "properties", "ontology", "graph", "graph_inferred", "prefixes", "query_example", "instructions"],
            template=template
        )
        chain_sparql = prompt_template_sparql | llm
        response_sparql = chain_sparql.invoke({
            "nl_query": query,
            "classes": classes,
            "properties": properties,
            "ontology": ontology,
            "graph": graph,
            "graph_inferred": graph_inferred,
            "prefixes": prefixes,
            "query_example": query_example,
            "instructions": instructions
        })
        sparql_query = response_sparql.content.strip()

        if topic != "None":
            final_query = format_sql(query_template.format(generated_sparql_query=sparql_query, topic=topic))
        else:
            final_query = query_template_no_topic.format(generated_sparql_query=sparql_query)

        cursor = connection.connection.cursor()
        cursor.execute(final_query)
        result = cursor.fetchall()
        result_json = json.dumps(result)

        return jsonify({'result': json.loads(result_json), 'final_query': final_query}), 200

    except Exception as e:
        return jsonify({'error': str(e), 'final_query': final_query}), 400

@app.route('/config', methods=['GET', 'POST'])
def config():
    cursor = connection.connection.cursor()
    if request.method == 'POST':
        data = request.get_json()
        update_query = """
        UPDATE ontology_config SET 
            ontology_query = ?, property_query = ?, classes_query = ?, instructions = ?, 
            prefixes = ?, graph = ?, graph_inferred = ?, query_example = ?, 
            template = ?, query_template = ?, query_template_no_topic = ?, template_similarity = ?
        """
        cursor.execute(update_query, (
            data.get('ontology_query'),
            data.get('property_query'),
            data.get('classes_query'),
            data.get('instructions'),
            data.get('prefixes'),
            data.get('graph'),
            data.get('graph_inferred'),
            data.get('query_example'),
            data.get('template'),
            data.get('query_template'),
            data.get('query_template_no_topic'),
            data.get('template_similarity')
        ))
        connection.connection.commit()
        return jsonify({'message': 'Configuration updated successfully'}), 200

    cursor.execute("SELECT ONTOLOGY_QUERY, PROPERTY_QUERY, CLASSES_QUERY, INSTRUCTIONS, PREFIXES, GRAPH, GRAPH_INFERRED, QUERY_EXAMPLE, TEMPLATE, QUERY_TEMPLATE, QUERY_TEMPLATE_NO_TOPIC, TEMPLATE_SIMILARITY FROM ONTOLOGY_CONFIG")
    config = cursor.fetchone()
    return jsonify({
        'ontology_query': config[0],
        'property_query': config[1],
        'classes_query': config[2],
        'instructions': config[3],
        'prefixes': config[4],
        'graph': NEW_GRAPH,  # always reflect new graph
        'graph_inferred': NEW_GRAPH_INFERRED,
        'query_example': config[7],
        'template': config[8],
        'query_template': config[9],
        'query_template_no_topic': config[10],
        'template_similarity': config[11]
    }), 200

@app.route('/', methods=['GET'])
def root():
    return 'Embeddings API: Health Check Successful.', 200

def create_app():
    return app