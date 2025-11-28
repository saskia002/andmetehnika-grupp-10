import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError


print("\nStarting Iceberg initialization script\n")


conn = duckdb.connect("data.duckdb")  # persistent DB file
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

catalog = load_catalog(name="rest")
namespace = "default"
table_name = "forbes_2000"

try:
    catalog.create_namespace(namespace)
    print(f"Namespace '{namespace}' created")
except NamespaceAlreadyExistsError:
    print(f"⚠ Namespace '{namespace}' already exists, skipping creation")


arrow_reader = conn.sql("SELECT * FROM forbes_2000").arrow()
arrow_table = pa.Table.from_batches(arrow_reader)  # convert RecordBatchReader to Table

try:
    catalog.drop_table(f"{namespace}.{table_name}")
    print(f"NB! Table '{namespace}.{table_name}' existed and was dropped")
except NoSuchTableError:
    print(f"NB!  Table '{namespace}.{table_name}' did not exist, skipping drop")

table = catalog.create_table(
    identifier=f"{namespace}.{table_name}",
    schema=arrow_table.schema,
)
print(f"Table '{namespace}.{table_name}' created")

table.append(arrow_table)
print(f"Data appended to '{namespace}.{table_name}'")


print("\nIceberg initialization completed successfully\n")