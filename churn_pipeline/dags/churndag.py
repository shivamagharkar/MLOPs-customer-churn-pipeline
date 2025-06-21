from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Project root path inside Astro container
PROJECT_ROOT = "/usr/local/airflow"

# Paths for dbt and ML script
DBT_PATH = os.path.join(PROJECT_ROOT, "transform/churn_dbt_project")
MODEL_SCRIPT = os.path.join(PROJECT_ROOT, "model/train_model.py")
PROFILES_DIR = os.path.join(PROJECT_ROOT, ".dbt")

default_args = {
    "owner": "shivam",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="churn_ml_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ml", "dbt", "snowflake"],
) as dag:

    # Step 1: Run dbt models
    dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"cd {DBT_PATH} && dbt run --profiles-dir {PROFILES_DIR}"
    )

    # Step 2: Run model training + predictions
    train_and_predict = BashOperator(
        task_id="train_model_and_save_predictions",
        bash_command=f"python {MODEL_SCRIPT}"
    )

    dbt_run >> train_and_predict
