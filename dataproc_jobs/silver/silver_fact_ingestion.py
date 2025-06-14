from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, coalesce, when, round
import os
import argparse
import json
from utils import create_spark_session, load_schema, read_data_from_gcs, add_metadata, write_to_bigquery
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, TimestampType

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Silver Fact Ingestion Job.")
    parser.add_argument('--project_id', type=str, required=True, help='GCP Project ID')
    args = parser.parse_args()

    project_id = args.project_id
    bq_bronze_dataset_id = "batch_pipeline_bronze"
    bq_silver_dataset_id = "batch_pipeline_silver"

    spark = create_spark_session(app_name="SilverFactIngestion")
    spark.conf.set("parentProject", project_id)

    silver_fact_schema_config_local_path = "silver_fact_schemas.json"

    with open(silver_fact_schema_config_local_path, 'r') as f:
        all_silver_fact_configs = json.load(f)

    for table_name, schema_definition_list in all_silver_fact_configs.items():
        bronze_input_table_ref = f"{project_id}.{bq_bronze_dataset_id}.{table_name}"
        silver_output_table_ref = f"{project_id}.{bq_silver_dataset_id}.{table_name}"

        target_silver_spark_schema = StructType([
            StructField(col_def["column_name"], globals()[col_def["data_type"].capitalize() + "Type"](), True)
            for col_def in schema_definition_list
        ])
        target_silver_spark_schema.add("ingestion_timestamp", TimestampType(), True)
        target_silver_spark_schema.add("source_table", StringType(), True)

        try:
            df_bronze = spark.read.format("bigquery").option("table", bronze_input_table_ref).load()
        except Exception as e:
            continue

        df_silver = df_bronze

        for column_field in df_silver.schema.fields:
            col_name = column_field.name
            col_spark_type = column_field.dataType
            if isinstance(col_spark_type, StringType):
                df_silver = df_silver.withColumn(col_name, coalesce(col(col_name), lit("Na")))
            elif isinstance(col_spark_type, (IntegerType, FloatType, LongType)):
                df_silver = df_silver.withColumn(col_name, coalesce(col(col_name), lit(0)))

        if table_name == "orders":
            df_silver = df_silver.drop_duplicates(['order_id'])
            if "order_date" in df_silver.columns:
                df_silver = df_silver.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
            if "total_amount" in df_silver.columns:
                df_silver = df_silver.withColumn("total_amount", coalesce(col("total_amount").cast(FloatType()), lit(0.0)))

        elif table_name == "order_items":
            df_silver = df_silver.drop_duplicates(['order_item_id'])
            if "quantity" in df_silver.columns:
                df_silver = df_silver.withColumn("quantity", coalesce(col("quantity").cast(IntegerType()), lit(0)))
            else:
                df_silver = df_silver.withColumn("quantity", lit(0).cast(IntegerType()))
            if "price" in df_silver.columns:
                df_silver = df_silver.withColumn("price", coalesce(col("price").cast(FloatType()), lit(0.0)))
            else:
                df_silver = df_silver.withColumn("price", lit(0.0).cast(FloatType()))
            if "item_total" not in df_silver.columns:
                df_silver = df_silver.withColumn("item_total", round(col("quantity") * col("price"), 2).cast(FloatType()))
            else:
                df_silver = df_silver.withColumn("item_total", coalesce(col("item_total").cast(FloatType()), lit(0.0)))
                df_silver = df_silver.withColumn("item_total",
                    when(col("item_total") == 0.0, round(col("quantity") * col("price"), 2)).otherwise(col("item_total"))
                )
            df_silver = df_silver.withColumn("item_total", col("item_total").cast(FloatType()))

        elif table_name == "payments":
            df_silver = df_silver.drop_duplicates(['payment_id'])
            if "payment_date" in df_silver.columns:
                df_silver = df_silver.withColumn("payment_date", to_date(col("payment_date"), "yyyy-MM-dd"))
            if "payment_amount" in df_silver.columns:
                df_silver = df_silver.withColumn("payment_amount", coalesce(col("payment_amount").cast(FloatType()), lit(0.0)))

        df_silver_with_metadata = add_metadata(df_silver, table_name)
        final_cols_with_metadata = [f.name for f in target_silver_spark_schema.fields]
        df_final_silver = df_silver_with_metadata.select([col(c).cast(target_silver_spark_schema[c].dataType) for c in final_cols_with_metadata])
        write_to_bigquery(df_final_silver, project_id, bq_silver_dataset_id, table_name)

    spark.stop()
