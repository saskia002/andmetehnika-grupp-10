from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import logging

def run_clickhouse_script(script_path: str, container_name="clickhouse", **context):
    """
    Executes a SQL script inside the ClickHouse container
    by piping it from Airflow container.
    """
    logging.info(f"Running ClickHouse script: {script_path}")

    try:
        # Use cat to pipe SQL file into ClickHouse
        result = subprocess.run(
            f"cat {script_path} | docker exec -i {container_name} clickhouse-client",
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        logging.info(result.stdout)
        if result.stderr:
            logging.warning(result.stderr)
        return result.stdout

    except subprocess.CalledProcessError as e:
        logging.error(f"ClickHouse script failed: {script_path}")
        logging.error(e.stderr)
        raise

# --- DAG definition ---
with DAG(
    dag_id="run_clickhouse_views",
    start_date=datetime(2025, 10, 26),
    schedule=None,  # manually triggered or triggered by other DAGs
    catchup=False,
    description="Run ClickHouse SQL scripts to refresh Silver/Gold layers",
) as dag:

    # --- Tasks ---
    create_roles_users = PythonOperator(
        task_id="create_roles_users",
        python_callable=run_clickhouse_script,
        op_kwargs={"script_path": "/opt/airflow/clickhouse/Privacy_and_Security/01_Create_roles_users.sql"}
    )

    create_full_views = PythonOperator(
        task_id="create_full_views",
        python_callable=run_clickhouse_script,
        op_kwargs={"script_path": "/opt/airflow/clickhouse/Privacy_and_Security/02_Create_full_views.sql"}
    )

    create_limited_views = PythonOperator(
        task_id="create_limited_views",
        python_callable=run_clickhouse_script,
        op_kwargs={"script_path": "/opt/airflow/clickhouse/Privacy_and_Security/03_Create_limited_views.sql"}
    )

    grant_access = PythonOperator(
        task_id="grant_access",
        python_callable=run_clickhouse_script,
        op_kwargs={"script_path": "/opt/airflow/clickhouse/Privacy_and_Security/04_Grant_access_to_roles.sql"}
    )

    # --- Task dependencies ---
    create_roles_users >> create_full_views >> create_limited_views >> grant_access
