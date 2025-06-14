from airflow import DAG
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'EM_Raw_Data_Generator',
    default_args=default_args,
    schedule_interval='0 10 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    trigger_dim = CloudFunctionInvokeFunctionOperator(
        task_id='trigger_dim',
        project_id='ecommerce-batch-pipeline-dbt',
        location='asia-south1',
        input_data={},  # Optional payload for POST
        function_id='generate_dim_data_cf'
    )

    trigger_fact = CloudFunctionInvokeFunctionOperator(
        task_id='trigger_fact',
        project_id='ecommerce-batch-pipeline-dbt',
        location='asia-south1',
        input_data={},
        function_id='generate_fact_data_cf'
    )

    trigger_dim >> trigger_fact
