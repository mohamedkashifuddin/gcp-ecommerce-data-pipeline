from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta

PROJECT_ID = "ecommerce-batch-pipeline-dbt"
REGION = "europe-west1"
PYSPARK_UTILS = "gs://ecommerce-batch-pipeline-dataproc-jobs/dataproc_jobs/utils/utils.py"
SERVICE_ACCOUNT = "dbt-bigquery-access@ecommerce-batch-pipeline-dbt.iam.gserviceaccount.com"

JOBS = [
    {
        "task_id": "bronze_dim_ingestion",
        "cluster": "bronze-dim-cluster-{{ ds_nodash }}",
        "script": "gs://ecommerce-batch-pipeline-dataproc-jobs/dataproc_jobs/bronze/bronze_dim_ingestion.py",
        "schema": "gs://ecommerce-batch-pipeline-dataproc-jobs/config/dim_schemas.json",
        "python_file_uris": [PYSPARK_UTILS],
    },
    {
        "task_id": "bronze_fact_ingestion",
        "cluster": "bronze-fact-cluster-{{ ds_nodash }}",
        "script": "gs://ecommerce-batch-pipeline-dataproc-jobs/dataproc_jobs/bronze/bronze_fact_ingestion.py",
        "schema": "gs://ecommerce-batch-pipeline-dataproc-jobs/config/fact_schemas.json",
        "python_file_uris": [PYSPARK_UTILS],
    },
    {
        "task_id": "silver_dim_ingestion",
        "cluster": "silver-dim-cluster-{{ ds_nodash }}",
        "script": "gs://ecommerce-batch-pipeline-dataproc-jobs/dataproc_jobs/silver/silver_dim_ingestion.py",
        "schema": "gs://ecommerce-batch-pipeline-dataproc-jobs/config/silver_dim_schemas.json",
        "python_file_uris": [PYSPARK_UTILS],
    },
    {
        "task_id": "silver_fact_ingestion",
        "cluster": "silver-fact-cluster-{{ ds_nodash }}",
        "script": "gs://ecommerce-batch-pipeline-dataproc-jobs/dataproc_jobs/silver/silver_fact_ingestion.py",
        "schema": "gs://ecommerce-batch-pipeline-dataproc-jobs/config/silver_fact_schemas.json",
        "python_file_uris": [PYSPARK_UTILS],
    },
]

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with models.DAG(
    dag_id="ecommerce_batch_ingestion_dag_16",
    description="Runs Bronze → Silver layer PySpark jobs sequentially using Dataproc",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["ecommerce", "batch", "spark"],
) as dag:

    previous_delete_task = None

    for job in JOBS:
        create = DataprocCreateClusterOperator(
            task_id=f"create_cluster_{job['task_id']}",
            project_id=PROJECT_ID,
            region=REGION,
            cluster_name=job["cluster"],
            cluster_config={
                "gce_cluster_config": {
                    "service_account": SERVICE_ACCOUNT
                },
                "master_config": {
                    "num_instances": 1,
                    "machine_type_uri": "n1-standard-2",
                    "disk_config": {"boot_disk_size_gb": 60}
                },
                "worker_config": {
                    "num_instances": 2,
                    "machine_type_uri": "n1-standard-2",
                    "disk_config": {"boot_disk_size_gb": 60}
                },
                "software_config": {
                    "image_version": "2.0-debian10"
                },
                "lifecycle_config": {
                    "idle_delete_ttl": {"seconds": 300}
                },
            },
        )

        extra_props = {
            "spark.jars.packages": "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0",
            "spark.hadoop.google.cloud.project.id": PROJECT_ID,
            "spark.conf.temporaryGcsBucket": "dataproc-temp-asia-south1-444088578532-n3nn2v7v"
        }

        submit = DataprocSubmitJobOperator(
            task_id=f"submit_{job['task_id']}",
            project_id=PROJECT_ID,
            region=REGION,
            job={
                "placement": {"cluster_name": job["cluster"]},
                "pyspark_job": {
                    "main_python_file_uri": job["script"],
                    "args": ["--project_id", PROJECT_ID],
                    "python_file_uris": job["python_file_uris"],
                    "file_uris": [job["schema"]],
                    "properties": extra_props,
                },
            },
        )

        delete = DataprocDeleteClusterOperator(
            task_id=f"delete_cluster_{job['task_id']}",
            project_id=PROJECT_ID,
            region=REGION,
            cluster_name=job["cluster"],
            trigger_rule=TriggerRule.ALL_DONE,
        )

        if previous_delete_task:
            previous_delete_task >> create
        create >> submit >> delete
        previous_delete_task = delete
