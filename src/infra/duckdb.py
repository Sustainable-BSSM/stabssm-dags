import duckdb

from src.common.config.s3 import S3Config


def create_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"""
        SET s3_region='{S3Config.REGION}';
        SET s3_access_key_id='{S3Config.ACCESS_KEY}';
        SET s3_secret_access_key='{S3Config.SECRET_KEY}';
    """)
    return conn
