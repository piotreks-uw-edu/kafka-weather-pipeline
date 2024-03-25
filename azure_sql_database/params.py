import os

# SOURCES
AZURE_SQL_DATABASE = os.environ.get('AZURE_SQL_DATABASE')
AZURE_SQL_USER = os.environ.get('AZURE_SQL_USER')
AZURE_SQL_PASSWORD = os.environ.get('AZURE_SQL_PASSWORD')
AZURE_SQL_SERVER = os.environ.get('AZURE_SQL_SERVER')
AZURE_SQL_PORT = os.environ.get('AZURE_SQL_PORT')

AZURE_DATABASE_URL = f"mssql+pymssql://{AZURE_SQL_USER}:{AZURE_SQL_PASSWORD}@{AZURE_SQL_SERVER}:{AZURE_SQL_PORT}/{AZURE_SQL_DATABASE}"