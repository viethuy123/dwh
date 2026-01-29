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

def extract_mongo_data_chunked(mongo_uri: str, mongo_db: str, mongo_collection: str, chunk_size: int):
    """
    Generator function để đọc MongoDB theo chunks
    Yields: DataFrame chunks
    """
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db[mongo_collection]
    
    skip = 0
    try:
        while True:
            cursor = collection.find().skip(skip).limit(chunk_size)
            chunk_data = list(cursor)
            
            if not chunk_data:
                break
            
            yield pd.DataFrame(chunk_data)
            skip += chunk_size
    finally:
        client.close()


def extract_sql_data_chunked(sql_uri: str, query: str, chunk_size: int):
    """
    Generator function để đọc SQL theo chunks
    Yields: DataFrame chunks
    """
    from sqlalchemy import pool
    
    engine = create_engine(sql_uri, poolclass=pool.NullPool, echo=False)
    
    try:
        with engine.connect().execution_options(
            stream_results=True,
            max_row_buffer=chunk_size
        ) as conn:
            yield from pd.read_sql(query, conn, chunksize=chunk_size)
    finally:
        engine.dispose()
