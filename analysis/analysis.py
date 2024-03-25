from azure_sql_database.params import AZURE_DATABASE_URL
from azure_sql_database.model import Database, Correlation

database = Database(AZURE_DATABASE_URL)

def get_correlations():
    with database.session() as session:
        correlation = session.query(Correlation).all()
    return str(correlation[0])