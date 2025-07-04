from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine


def extract_mongo_data(mongo_uri: str, mongo_db: str, mongo_collection: str) -> pd.DataFrame:
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db[mongo_collection]
    df = pd.DataFrame(list(collection.find()))
    return df

def extract_sql_data(sql_uri: str, query: str) -> pd.DataFrame:
    engine = create_engine(sql_uri)
    with engine.connect() as conn: # type: ignore
        try:
            df = pd.read_sql(query, conn)
        except Exception as e:
            print(f"Error reading SQL data: {e}")
            raise
    return df
