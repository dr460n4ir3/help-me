from flask import Flask, request, jsonify, render_template
import sqlite3
import pandas as pd
import os

app = Flask(__name__)

# Ensure the upload directory exists
if not os.path.exists('uploads'):
    os.makedirs('uploads')

# Function to connect to SQLite
def connect_to_sqlite():
    conn = sqlite3.connect('uploaded_csv.db')
    return conn

# Function to initialize SQLite database
def initialize_sqlite_db():
    conn = connect_to_sqlite()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS uploaded_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        )
    ''')
    conn.commit()
    conn.close()

# Initialize the SQLite database
initialize_sqlite_db()

# Function to check if a column exists in the table
def column_exists(conn, table_name, column_name):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in cursor.fetchall()]
    return column_name in columns

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        app.logger.error("No file part in the request")
        return "No file part", 400
    file = request.files['file']
    if file.filename == '':
        app.logger.error("No selected file")
        return "No selected file", 400
    file_path = os.path.join('uploads', file.filename)
    file.save(file_path)
    try:
        df = pd.read_csv(file_path)
        
        # Drop any unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        
        # Dynamically create columns in the SQLite table based on the CSV file headers
        conn = connect_to_sqlite()
        cursor = conn.cursor()
        for column in df.columns:
            if not column_exists(conn, 'uploaded_data', column):
                cursor.execute(f'ALTER TABLE uploaded_data ADD COLUMN {column} TEXT')
        df.to_sql('uploaded_data', conn, if_exists='append', index=False)
        conn.close()
        
        app.logger.info("File uploaded successfully")
        return "File uploaded successfully", 200
    except Exception as e:
        app.logger.error(f"Error uploading file: {e}")
        return f"Error uploading file: {e}", 500

@app.route('/query', methods=['POST'])
def query_sqlite():
    sql_query = request.form['sql']
    app.logger.info(f"Executing SQL query: {sql_query}")
    try:
        conn = connect_to_sqlite()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchall()
        conn.close()
        app.logger.info(f"Query executed successfully, result: {result}")
        return jsonify({"result": result})
    except Exception as e:
        app.logger.error(f"Error executing query: {e}")
        return f"Error executing query: {e}", 500

app.run(host="0.0.0.0", port=4444, debug=True)
