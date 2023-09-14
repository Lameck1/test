import json
from flask import Flask, jsonify, request, Response
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage
from database import get_db, close_db, execute_query
import logging
from urllib.parse import urlparse
import os
from io import BytesIO
import numpy as np
import pandas as pd
import requests
import re
from datetime import datetime, date
from json import JSONEncoder as BaseJSONEncoder
from pandas._libs.tslibs.nattype import NaTType
from data_cleaning import clean_and_store_data
from data_processing import get_sql_query, get_assistant_response, generate_charts
from query_processing import (
    extract_intent_and_respond,
    search_hdx_database,
    generate_keywords_with_chatgpt,
    formualte_question
)

logging.basicConfig(filename="/tmp/app.log", level=logging.DEBUG)

class JSONEncoder(BaseJSONEncoder):
    def default(self, obj):
        if isinstance(obj, NaTType):
            return None
        elif isinstance(obj, np.generic):
            if np.isnan(obj):
                return None
            return obj.item()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {key: self.default(value) for key, value in obj.items()}
        return super().default(obj)

app = Flask(__name__)
app.json_encoder = JSONEncoder

# Define a list of allowed file extensions
ALLOWED_EXTENSIONS = set(['csv'])

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.before_request
def before_request():
    get_db()

@app.teardown_request
def teardown_request(exception):
    close_db(exception)

@app.route("/analyze_data/<upload_id>", methods=["POST"])
def analyze_data(upload_id):
    try:
        payload = request.get_json()
        user_query = payload.get("query")

        if not user_query:
            return jsonify({"error": "No query provided"}), 400

        cursor = get_db().cursor()
        cursor.execute(f"DESCRIBE {upload_id};")
        columns = cursor.fetchall()
        column_names = ", ".join([column[0] for column in columns])

        cleaned_sql_query = get_sql_query(user_query, column_names, upload_id)

        messages = []
        if cleaned_sql_query:
            query_results = execute_query(cleaned_sql_query)

            assistant_response = get_assistant_response(
                user_query, cleaned_sql_query, query_results
            )
            messages.append({"role": "assistant", "content": f"{assistant_response}"})

            chart_string = generate_charts(query_results)
            if chart_string:
                messages.append({"role": "chart", "content": chart_string})

            return jsonify({"messages": messages})
        else:
            messages.append(
                {
                    "role": "assistant",
                    "content": f"The selected dataset doesn't have an answer for the asked question",
                }
            )
            return jsonify({"messages": messages})
    except Exception as e:
        error_message = str(e)
        logging.error(f"Error analyzing data: {error_message}", exc_info=True)
        return jsonify({"error": f"Error analyzing data: {error_message}"}), 500

@app.route("/ask_for_datasets", methods=["POST"])
def search():
    try:
        messages = []
        payload = request.get_json()
        user_question = payload.get("query")
        if not user_question:
            return jsonify({"error": "No query provided"}), 400
        
        formulated_question = formualte_question(user_question)
        print(formulated_question)

        keywords = generate_keywords_with_chatgpt(formulated_question)

        query = "+".join(re.sub(r"[^a-zA-Z0-9]+", " ", keywords).split())

        datasets = search_hdx_database(query)

        messages.append(
            {
                "role": "datasets",
                "result": datasets,
                "content": extract_intent_and_respond(user_question),
                "formulated_quiz": formulated_question
            }
        )
        return jsonify({"messages": messages})

    except Exception as e:
        error_message = str(e)
        logging.error(f"Error in ask_for_datasets: {error_message}", exc_info=True)
        return jsonify({"error": f"Error in ask_for_datasets: {error_message}"}), 500

@app.route("/select_dataset", methods=["POST"])
def select_dataset():
    try:
        download_url = request.json["download_url"]
        messages = []
        response = requests.get(download_url, allow_redirects=True)
        if response.status_code != 200:
           return jsonify({"error": f"Error downloading file from {download_url}"}), 400
        file = FileStorage(
            stream=BytesIO(response.content),
            filename=download_url.split("/")[-1],
            content_type=response.headers["content-type"],
        )

        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
            file.save(file_path)
            remove_second_line_if_starts_with_hash(file_path)
            conn = get_db()

            upload_id, sample_data = clean_and_store_data(file_path, file, conn)

            if upload_id is not None and not sample_data.empty:
                json_sample_data = sample_data.to_json(
                    orient="records",
                    date_format="iso",
                    default_handler=JSONEncoder().default,
                )
                messages.append(
                    {
                        "status": "success",
                        "role": "assistant",
                        "result": {
                            "upload_id": upload_id,
                            "sample_data": json.loads(json_sample_data),
                        },
                        "content": f"Dataset cleaned."
                    }
                )
                return jsonify({"messages": messages}), 200
            else:
                error_message = "Error processing file"
                logging.error(error_message)
                return jsonify({"error": error_message}), 400
        else:
            error_message = "Unsupported file type"
            logging.error(error_message)
            return jsonify({"error": error_message}), 400

    except Exception as e:
        error_message = str(e)
        logging.error(f"Unhandled exception: {error_message}", exc_info=True)
        return jsonify({"error": f"Unhandled exception: {error_message}"}), 500

def remove_second_line_if_starts_with_hash(file_name):
    with open(file_name, 'r') as file:
        lines = file.readlines()

    if len(lines) > 1 and lines[1].startswith('#'):
        del lines[1]

    with open(file_name, 'w') as file:
        file.writelines(lines)