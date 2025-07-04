import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.extract_data import extract_mongo_data, extract_sql_data

def test_extract_mongo_data() -> None:
    mongo_uri = "mongodb://datawarehouse_tio:H#nB53k6rGuso6d6@150.95.109.100:27017/portal"
    mongo_db = "portal"
    mongo_collection = "projects"
    df = extract_mongo_data(mongo_uri, mongo_db, mongo_collection)
    assert df.shape[0] > 0

def test_extract_sql_data() -> None:
    sql_uri = "mysql+pymysql://root:Gmo2024@103.18.6.157:7001/jira8db"
    query = "SELECT * FROM project"
    df = extract_sql_data(sql_uri, query)
    assert df.shape[0] > 0