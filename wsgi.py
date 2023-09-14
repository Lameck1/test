from app import app
from database import init_db
from flask_cors import CORS
from config import Config  # import the Config class
import os
import logging
from dotenv import load_dotenv

load_dotenv()

# Check if the 'database' directory exists
if not os.path.exists('/tmp/database'):
    # Create the 'database' directory
    os.makedirs('/tmp/database')
    logging.info("Created 'database' directory.")

# Check if the 'uploads' directory exists
if not os.path.exists('/tmp/uploads'):
    # Create the 'uploads' directory
    os.makedirs('/tmp/uploads')
    logging.info("Created 'uploads' directory.")

app.config.from_object(Config)  # load configurations from the Config class
CORS(app)  # This will enable CORS for all routes

with app.app_context():
    db_path = init_db()