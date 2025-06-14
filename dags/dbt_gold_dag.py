from airflow import models
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with models.DAG(
    dag_id="run_dbt_gold_layer_3",
    default_args=default_args,
    description="Run dbt gold layer models using BashOperator",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["dbt", "gold", "bigquery"],
) as dag:

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="""
        export GOOGLE_APPLICATION_CREDENTIALS=/home/airflow/gcs/data/dbt/ecommerce-batch-pipeline-dbt.json
        mkdir -p ~/.dbt
        cp /home/airflow/gcs/data/dbt/profiles.yml ~/.dbt/

        cd /home/airflow/gcs/data/dbt/gold_layer_dbt
        dbt deps
        dbt run --profiles-dir ~/.dbt --profile ecommerce_gold --project-dir .
        """
        ,
    )

    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command="""
        export GOOGLE_APPLICATION_CREDENTIALS=/home/airflow/gcs/data/dbt/ecommerce-batch-pipeline-dbt.json
        cd /home/airflow/gcs/data/dbt/gold_layer_dbt
        dbt test --profiles-dir ~/.dbt --profile ecommerce_gold --project-dir .
        """,
    )

    run_dbt >> test_dbt
