from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, lower, upper, initcap, regexp_replace, concat_ws, to_date, coalesce, when, isnull, split, element_at, size, slice, trim, expr
import os
import argparse
import json

from utils import create_spark_session, load_schema, read_data_from_gcs, add_metadata, write_to_bigquery

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, TimestampType

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Silver Dimension Ingestion Job.")
    parser.add_argument('--project_id', type=str, required=True, help='GCP Project ID')
    args = parser.parse_args()

    project_id = args.project_id
    bq_bronze_dataset_id = "batch_pipeline_bronze"
    bq_silver_dataset_id = "batch_pipeline_silver"

    spark = create_spark_session(app_name="SilverDimIngestion")
    spark.conf.set("parentProject", project_id)

    silver_dim_schema_config_local_path = "silver_dim_schemas.json"

    with open(silver_dim_schema_config_local_path, 'r') as f:
        all_silver_dim_configs = json.load(f)

    dim_tables = all_silver_dim_configs.keys()

    for table_name in dim_tables:
        print(f"\n--- Processing {table_name} for Silver Layer ---")
        bronze_input_table_ref = f"{project_id}.{bq_bronze_dataset_id}.{table_name}"
        silver_output_table_ref = f"{project_id}.{bq_silver_dataset_id}.{table_name}"

        silver_schema_definition_list = all_silver_dim_configs[table_name]

        target_silver_spark_schema = StructType([
            StructField(col_def["column_name"], globals()[col_def["data_type"].capitalize() + "Type"](), True)
            for col_def in silver_schema_definition_list
        ])
        target_silver_spark_schema.add("ingestion_timestamp", TimestampType(), True)
        target_silver_spark_schema.add("source_table", StringType(), True)

        print(f"Reading from Bronze table: {bronze_input_table_ref}")
        try:
            df_bronze = spark.read \
                .format("bigquery") \
                .option("table", bronze_input_table_ref) \
                .load()
            print(f"Read {df_bronze.count()} records from Bronze {table_name}.")
            print("Bronze Schema:")
            df_bronze.printSchema()
        except Exception as e:
            print(f"Error reading Bronze table {table_name} from BigQuery: {e}. Skipping.")
            continue

        df_silver = df_bronze

        for column_field in df_silver.schema.fields:
            col_name = column_field.name
            col_spark_type = column_field.dataType
            if isinstance(col_spark_type, StringType):
                df_silver = df_silver.withColumn(col_name, coalesce(col(col_name), lit("Na")))
            elif isinstance(col_spark_type, (IntegerType, FloatType, LongType)):
                df_silver = df_silver.withColumn(col_name, coalesce(col(col_name), lit(0)))

        if table_name == "customers":
            df_silver = df_silver.drop_duplicates(['customer_id'])

            if "customer_name" in df_silver.columns:
                df_silver = df_silver.withColumn("name_parts", split(col("customer_name"), " ")) \
                                     .withColumn("first_name", element_at(col("name_parts"), 1))

                df_silver = df_silver.withColumn("last_name",
                                                 when(size(col("name_parts")) > 1,
                                                      concat_ws(" ", expr("slice(name_parts, 2, size(name_parts) - 1)"))
                                                      ).otherwise(lit(None).cast(StringType()))
                                                 ) \
                    .drop("name_parts", "customer_name")
            else:
                print(f"Warning: 'customer_name' not found in Bronze '{table_name}'. Skipping name split.")
                df_silver = df_silver.withColumn("first_name", lit(None).cast(StringType())) \
                                     .withColumn("last_name", lit(None).cast(StringType()))

            df_silver = df_silver \
                .withColumn("first_name", initcap(col("first_name"))) \
                .withColumn("last_name", initcap(col("last_name"))) \
                .withColumn("email", lower(col("email")))

            df_silver = df_silver.withColumn("full_name",
                                             when(col("last_name").isNull() | (trim(col("last_name")) == ""), col("first_name"))
                                             .otherwise(concat_ws(" ", col("first_name"), col("last_name")))
                                            )

            if "join_date" in df_silver.columns:
                df_silver = df_silver.withColumn("join_date", to_date(col("join_date"), "yyyy-MM-dd"))
            else:
                print(f"Warning: 'join_date' not found in Bronze '{table_name}'. Skipping date conversion.")

        elif table_name == "products":
            df_silver = df_silver.drop_duplicates(['product_id'])

            df_silver = df_silver \
                .withColumn("product_name", lower(col("product_name"))) \
                .withColumn("category", lower(col("category")))

            if "price" in df_silver.columns:
                df_silver = df_silver.withColumn("price", col("price").cast(FloatType()))
            else:
                print(f"Warning: 'price' not found in Bronze '{table_name}'. Skipping price cast.")

        elif table_name == "dates":
            df_silver = df_silver.drop_duplicates(['date'])

            if "date_id" in df_silver.columns:
                df_silver = df_silver.drop("date_id")
            else:
                print(f"Warning: 'date_id' not found in Bronze '{table_name}'. Skipping drop.")

            if "date" in df_silver.columns:
                df_silver = df_silver.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
            else:
                print(f"Warning: 'date' not found in Bronze '{table_name}'. Skipping date conversion.")

            df_silver = df_silver \
                .withColumn("day_of_week", lower(col("day_of_week"))) \
                .withColumn("month", lower(col("month")))

        final_cols_with_metadata = [f.name for f in target_silver_spark_schema.fields]
        df_silver = df_silver.select([col(c).cast(target_silver_spark_schema[c].dataType) for c in final_cols_with_metadata])

        print(f"Writing to Silver table: {silver_output_table_ref}")
        write_to_bigquery(df_silver, project_id, bq_silver_dataset_id, table_name)
        print(f"Silver layer processing for {table_name} completed. Data written to: {silver_output_table_ref}")
        print(f"Total records in Silver {table_name}: {df_silver.count()}")
        print("Silver Schema (after transformations and casting):")
        df_silver.printSchema()

    spark.stop()
