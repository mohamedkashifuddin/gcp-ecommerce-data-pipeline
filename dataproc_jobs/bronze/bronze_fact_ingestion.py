from pyspark.sql.functions import to_date, col
import argparse
import json
import os
from utils import create_spark_session, load_schema, read_data_from_gcs, add_metadata, write_to_bigquery

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Bronze Fact Ingestion Job.")
    parser.add_argument('--project_id', type=str, required=True, help='GCP Project ID')
    args = parser.parse_args()

    project_id = args.project_id
    bq_dataset_id = "batch_pipeline_bronze"
    spark = create_spark_session(app_name="BronzeFactIngestion")
    spark.conf.set("parentProject", project_id)

    fact_schema_config_local_path = "fact_schemas.json"
    raw_data_gcs_base_path = "gs://ecommerce_batch_pipeline_raw-data/fact/"

    with open(fact_schema_config_local_path, 'r') as f:
        all_fact_configs = json.load(f)

    if "facts" not in all_fact_configs or not isinstance(all_fact_configs["facts"], list):
        spark.stop()
        exit(1)

    for fact_config in all_fact_configs["facts"]:
        table_name = fact_config["table_name"]
        file_pattern = os.path.join(raw_data_gcs_base_path, f"{table_name}_*.csv")
        spark_schema = load_schema(fact_schema_config_local_path, table_name, schema_type="facts")

        try:
            df_raw = read_data_from_gcs(spark, file_pattern, spark_schema)

            if df_raw.count() > 0:
                if table_name == "orders":
                    if "order_date" in df_raw.columns:
                        df_raw = df_raw.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
                elif table_name == "payments":
                    if "payment_date" in df_raw.columns:
                        df_raw = df_raw.withColumn("payment_date", to_date(col("payment_date"), "yyyy-MM-dd"))

                df_with_meta = add_metadata(df_raw, table_name)
                write_to_bigquery(df_with_meta, project_id, bq_dataset_id, table_name)
        except Exception as e:
            pass

    spark.stop()
