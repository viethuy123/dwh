from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine


def extract_mongo_data(mongo_uri: str, mongo_db: str, mongo_collection: str) -> pd.DataFrame:
    """Full load — dùng _id pagination gom lại thành 1 DataFrame."""
    chunks = list(extract_mongo_data_chunked(mongo_uri, mongo_db, mongo_collection, chunk_size=10_000))
    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)

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
    Generator dùng _id cursor pagination thay vì skip/limit.
    
    skip/limit phải scan từ đầu mỗi chunk → O(n²) với collection lớn.
    _id pagination dùng index có sẵn → O(n) tổng cộng.
    """
    from bson import ObjectId

    client = MongoClient(mongo_uri)
    try:
        collection = client[mongo_db][mongo_collection]
        last_id = None

        while True:
            query = {'_id': {'$gt': last_id}} if last_id else {}
            batch = list(collection.find(query).sort('_id', 1).limit(chunk_size))

            if not batch:
                break

            yield pd.DataFrame(batch)
            last_id = batch[-1]['_id']
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
