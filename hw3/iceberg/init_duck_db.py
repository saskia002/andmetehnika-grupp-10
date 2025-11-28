import duckdb

print("\nStarting DuckDB initialization script\n")

conn = duckdb.connect("data.duckdb")
conn.install_extension("httpfs")
conn.load_extension("httpfs")


conn.sql("""
SET s3_region='us-east-1';
SET s3_url_style='path';
SET s3_endpoint='minio:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
""")

tables = [
    "forbes_2000",
]

for table in tables:
    s3_path = 's3://bucket/forbes_2000_companies_2025.csv'
    conn.sql(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_csv('{s3_path}')")

print("\nDuckDB initialization completed successfully\n")
