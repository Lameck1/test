import os

class Config(object):
    UPLOAD_FOLDER = '/tmp/uploads'
    DATABASE_PATH = os.path.join('/tmp/database', 'data.duckdb')
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
