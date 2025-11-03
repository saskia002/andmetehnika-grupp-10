from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import logging
import time

def run_dbt_command(command: str, selector: str = None, **context):
    """
    Run dbt command in the dbt container.
    Commands: 'snapshot', 'run', or 'test'
    """
    dbt_container = "dbt"
    dbt_project_dir = "/dbt"
    
    if command == "snapshot":
        dbt_cmd = ["dbt", "snapshot", "--profiles-dir", dbt_project_dir, "--project-dir", dbt_project_dir]
        task_description = "Running dbt snapshots"
    elif command == "run":
        dbt_cmd = ["dbt", "run", "--profiles-dir", dbt_project_dir, "--project-dir", dbt_project_dir]
        if selector:
            dbt_cmd += ["--select", selector]
        task_description = f"Running dbt models ({selector or 'all'})"
    elif command == "test":
        dbt_cmd = ["dbt", "test", "--profiles-dir", dbt_project_dir, "--project-dir", dbt_project_dir]
        task_description = "Running dbt tests"
    else:
        raise ValueError(f"Unknown dbt command: {command}")
    
    logging.info(task_description)
    logging.info(f"Command: dbt {command}")
    
    # First check if docker is available
    try:
        docker_check = subprocess.run(
            ["docker", "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        logging.info(f"Docker available: {docker_check.stdout.strip()}")
    except (FileNotFoundError, subprocess.TimeoutExpired):
        error_msg = "Docker command not available. Please ensure docker socket is mounted to Airflow containers."
        logging.error(error_msg)
        raise Exception(error_msg)
    
    # Check if dbt container is running
    try:
        container_check = subprocess.run(
            ["docker", "ps", "--filter", f"name={dbt_container}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True
        )
        if dbt_container not in container_check.stdout:
            error_msg = f"Container '{dbt_container}' is not running. Available containers: {container_check.stdout}"
            logging.error(error_msg)
            raise Exception(error_msg)
        logging.info(f"Container '{dbt_container}' is running")
    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to check container status: {e}"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    # Check if ClickHouse container is running and healthy (with retries)
    max_retries = 12  # Wait up to 2 minutes (12 * 10 seconds)
    retry_delay = 10  # seconds
    clickhouse_ready = False
    
    for attempt in range(1, max_retries + 1):
        try:
            clickhouse_check = subprocess.run(
                ["docker", "ps", "--filter", "name=clickhouse", "--format", "{{.Names}} {{.Status}}"],
                capture_output=True,
                text=True,
                timeout=5,
                check=True
            )
            
            if "clickhouse" not in clickhouse_check.stdout:
                if attempt < max_retries:
                    logging.warning(f"ClickHouse container not found. Waiting... (attempt {attempt}/{max_retries})")
                    time.sleep(retry_delay)
                    continue
                else:
                    error_msg = "ClickHouse container is not running. Please ensure ClickHouse is started."
                    logging.error(error_msg)
                    raise Exception(error_msg)
            
            status = clickhouse_check.stdout.strip()
            if "Restarting" in status or "Exited" in status:
                if attempt < max_retries:
                    logging.warning(f"ClickHouse container is not healthy: {status}. Waiting... (attempt {attempt}/{max_retries})")
                    time.sleep(retry_delay)
                    continue
                else:
                    error_msg = f"ClickHouse container is not healthy after {max_retries} attempts: {status}"
                    logging.error(error_msg)
                    logging.error("Please check ClickHouse logs: docker logs clickhouse")
                    raise Exception(error_msg)
            
            logging.info(f"ClickHouse container is running: {status}")
            
            # Test ClickHouse connectivity
            clickhouse_test = subprocess.run(
                ["docker", "exec", "clickhouse", "clickhouse-client", "--query", "SELECT 1"],
                capture_output=True,
                text=True,
                timeout=10,
                check=True
            )
            logging.info("ClickHouse connectivity test passed")
            clickhouse_ready = True
            break
            
        except subprocess.CalledProcessError as e:
            if attempt < max_retries:
                logging.warning(f"ClickHouse connectivity test failed (attempt {attempt}/{max_retries}): {e.stderr or e.stdout}. Retrying...")
                time.sleep(retry_delay)
                continue
            else:
                error_msg = f"ClickHouse is not accessible after {max_retries} attempts: {e.stderr or e.stdout}"
                logging.error(error_msg)
                logging.error("Please check ClickHouse logs: docker logs clickhouse")
                raise Exception(error_msg)
        except subprocess.TimeoutExpired:
            if attempt < max_retries:
                logging.warning(f"ClickHouse connectivity test timed out (attempt {attempt}/{max_retries}). Retrying...")
                time.sleep(retry_delay)
                continue
            else:
                error_msg = f"ClickHouse connectivity test timed out after {max_retries} attempts"
                logging.error(error_msg)
                raise Exception(error_msg)
        except Exception as e:
            # For other exceptions, don't retry
            raise
    
    if not clickhouse_ready:
        error_msg = f"ClickHouse did not become ready after {max_retries} attempts"
        logging.error(error_msg)
        raise Exception(error_msg)
    
    # Execute dbt command in container
    try:
        cmd = ["docker", "exec", dbt_container, "bash", "-c", f"cd {dbt_project_dir} && {' '.join(dbt_cmd)}"]
        logging.info(f"Executing: {' '.join(cmd[:3])} ... {dbt_cmd}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=600  # 10 minute timeout
        )
        
        # Log output
        logging.info(f"dbt {command} stdout:\n{result.stdout}")
        if result.stderr:
            logging.warning(f"dbt {command} stderr:\n{result.stderr}")
        
        # Check for dbt errors in output
        if "ERROR" in result.stdout or "FAIL" in result.stdout:
            logging.warning(f"dbt {command} completed with warnings/errors")
        
        logging.info(f"dbt {command} completed successfully")
        return result.stdout
        
    except subprocess.CalledProcessError as e:
        error_msg = f"dbt {command} failed with return code {e.returncode}"
        logging.error(error_msg)
        logging.error(f"stdout:\n{e.stdout}")
        logging.error(f"stderr:\n{e.stderr}")
        
        # Provide helpful debugging info
        logging.error(f"\nTo debug, run manually:")
        logging.error(f"  docker exec {dbt_container} bash -c 'cd {dbt_project_dir} && {' '.join(dbt_cmd)}'")
        
        # For test command, log warnings but don't fail the DAG (tests can have failures that are warnings)
        if command == "test":
            logging.warning(f"dbt tests completed with failures, but not blocking the DAG.")
            logging.warning(f"Review the test results above to identify data quality issues.")
            # Return the output instead of raising exception
            return e.stdout
        
        # For run command, raise exception as model failures are critical
        raise Exception(f"{error_msg}\n\nStdout: {e.stdout[:500]}\nStderr: {e.stderr[:500]}")
        
    except subprocess.TimeoutExpired:
        error_msg = f"dbt {command} timed out after 10 minutes"
        logging.error(error_msg)
        raise Exception(error_msg)
        
    except Exception as e:
        logging.error(f"Unexpected error running dbt {command}: {e}")
        raise

with DAG(
    dag_id="run_dbt_transformations",
    start_date=datetime(2025, 10, 26),
    schedule=None,  # Triggered by other DAGs
    catchup=False,
    description="Run dbt transformations to refresh Silver and Gold layers"
) as dag:
    
    # Run dbt transformations (staging models)
    run_dbt_staging = PythonOperator(
        task_id="run_dbt_staging",
        python_callable=run_dbt_command,
        op_kwargs={"command": "run","selector": "staging"},
    )

    # Run dbt snapshots (needed before DimCompany and DimTicker)
    run_dbt_snapshots = PythonOperator(
        task_id="run_dbt_snapshots",
        python_callable=run_dbt_command,
        op_kwargs={"command": "snapshot"},
    )

    # Run dbt marts (gold layer)
    run_dbt_marts = PythonOperator(
        task_id="run_dbt_marts",
        python_callable=run_dbt_command,
        op_kwargs={"command": "run","selector": "marts"},
    )
    
    # Optionally run dbt tests (non-blocking if tests fail)
    run_dbt_tests = PythonOperator(
        task_id="run_dbt_tests",
        python_callable=run_dbt_command,
        op_kwargs={"command": "test"},
        trigger_rule="all_done",  # Run even if models fail (for debugging)
    )

    # Set task dependencies - snapshots first, then models, then tests
    run_dbt_staging >> run_dbt_snapshots >> run_dbt_marts >> run_dbt_tests

