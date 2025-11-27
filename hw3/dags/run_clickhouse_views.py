from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import subprocess
import logging
import time

# running clickhouse scripts as called
def run_clickhouse_script(script_path: str, container_name="clickhouse", **context):
    import subprocess
    import logging

    cmd = ["docker", "exec", "-i", container_name, "clickhouse-client", "<", script_path]

    logging.info(f"Running ClickHouse script: {script_path}")

    try:
        result = subprocess.run(
            f"docker exec -i {container_name} clickhouse-client < {script_path}",
            shell=True,  # required for input redirection `<`
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


with DAG(
    dag_id="run_clickhouse_views",
    start_date=datetime(2025, 10, 26),
    schedule=None,  # Triggered by other DAGs
    catchup=False,
    description="Run dbt transformations to refresh Silver and Gold layers"
) as dag:
    
    # Wait for dbt marts DAG to finish
    wait_for_dbt_marts = ExternalTaskSensor(    
        task_id="wait_for_dbt_marts",
        external_dag_id="run_dbt_transformations",  # your dbt DAG id
        external_task_id="run_dbt_marts",           # the specific dbt task
        mode="poke",                                # or "reschedule" to free worker
        poke_interval=60,                           # check every 60 seconds
        timeout=3600                                # fail after 1 hour
    )

    # ClickHouse DAG tasks
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

    # --------------------------
    # Dependencies
    # --------------------------
    # Wait for dbt marts first
    wait_for_dbt_marts >> create_full_views

    # Roles can be created beforehand
    create_roles_users >> create_full_views

    # Then limited views and grants
    create_full_views >> create_limited_views >> grant_access

