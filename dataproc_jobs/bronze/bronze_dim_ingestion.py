from pyspark.sql.functions import col, lit, current_timestamp
import argparse
import json
import os
from utils import create_spark_session, load_schema, read_data_from_gcs, add_metadata, write_to_bigquery

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Bronze Dimension Ingestion Job.")
    parser.add_argument('--project_id', type=str, required=True, help='GCP Project ID')
    args = parser.parse_args()

    project_id = args.project_id
    bq_dataset_id = "batch_pipeline_bronze"
    spark = create_spark_session(app_name="BronzeDimIngestion")
    spark.conf.set("parentProject", project_id)

    dim_schema_config_local_path = "dim_schemas.json"
    raw_data_gcs_base_path = "gs://ecommerce_batch_pipeline_raw-data/dim/"

    with open(dim_schema_config_local_path, 'r') as f:
        all_dim_configs = json.load(f)

    if "dimensions" not in all_dim_configs or not isinstance(all_dim_configs["dimensions"], list):
        spark.stop()
        exit(1)

    for dim_config in all_dim_configs["dimensions"]:
        table_name = dim_config["table_name"]
        file_pattern = os.path.join(raw_data_gcs_base_path, f"{table_name}_*.csv")
        spark_schema = load_schema(dim_schema_config_local_path, table_name, schema_type="dimensions")

        try:
            df_raw = read_data_from_gcs(spark, file_pattern, spark_schema)
            if df_raw.count() > 0:
                df_with_meta = add_metadata(df_raw, table_name)
                write_to_bigquery(df_with_meta, project_id, bq_dataset_id, table_name)
        except Exception as e:
            pass

    spark.stop()
