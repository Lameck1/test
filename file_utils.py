import io
import csv

ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(file):
    return '.' in file and \
           file.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS