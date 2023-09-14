import duckdb
from flask import current_app, g
import logging

# Set up logging for the module
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_db():
    """
    This function initializes a DuckDB database connection.
    It then establishes a connection to the DuckDB database and stores this connection in the 
    global variable 'db_conn'. This allows other functions in the application to use this 
    database connection.
    
    Returns:
        DuckDB connection object
    """
    try:
        # Persistent DuckDB connection
        db_path = current_app.config['DATABASE_PATH']  # Get the path from Flask's app config
        db_conn = duckdb.connect(database=db_path, read_only=False)
        logger.info(f"Initialized persistent database connection: {db_conn}")
        return db_conn
    except Exception as e:
        logger.error(f"Error initializing database: {e}")


def get_db():
    if 'db' not in g:
        g.db = duckdb.connect(database=current_app.config['DATABASE_PATH'], read_only=False)
    return g.db

def execute_query(query, params=None, connection=None):
    db = connection if connection else get_db()
    cursor = db.cursor()
    
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        results = cursor.fetchall()
        return results
    
    except Exception as e:
        logger.error(f"Error executing query: {query}. Error: {e}")


def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()
