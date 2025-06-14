from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, DateType, TimestampType
import json
import os

def create_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.35.0")
        .config("temporaryGcsBucket", "ecommerce_batch_pipeline_dataproc-temp")
        .getOrCreate()
    )
    print(f"SparkSession '{app_name}' created successfully.")
    return spark

def get_spark_schema(json_schema_list):
    fields = []
    for field_def in json_schema_list:
        col_name = field_def["column_name"]
        data_type_str = field_def["data_type"].lower()
        if data_type_str == "string":
            spark_type = StringType()
        elif data_type_str == "integer":
            spark_type = IntegerType()
        elif data_type_str == "long":
            spark_type = LongType()
        elif data_type_str == "float":
            spark_type = FloatType()
        elif data_type_str == "date":
            spark_type = DateType()
        elif data_type_str == "timestamp":
            spark_type = TimestampType()
        else:
            raise ValueError(f"Unsupported data type: {data_type_str} for column {col_name}")
        fields.append(StructField(col_name, spark_type, True))
    return StructType(fields)

def load_schema(schema_config_path: str, table_name: str, schema_type: str = "facts"):
    print(f"Loading schema for table '{table_name}' from '{schema_config_path}' under '{schema_type}' key...")
    with open(schema_config_path, 'r') as f:
        configs = json.load(f)

    if schema_type not in configs or not isinstance(configs[schema_type], list):
        raise ValueError(f"'{schema_type}' key not found or not a list in {schema_config_path}")

    for config in configs[schema_type]:
        if config["table_name"] == table_name:
            spark_schema = get_spark_schema(config["schema"])
            print(f"Schema loaded for {table_name}: {spark_schema.jsonValue()}")
            return spark_schema
    raise ValueError(f"Schema for table '{table_name}' not found in {schema_config_path}")

def read_data_from_gcs(spark: SparkSession, file_pattern: str, schema: StructType) -> DataFrame:
    print(f"Reading data from: {file_pattern}")
    try:
        df = spark.read.csv(file_pattern, header=True, schema=schema)
        if df.rdd.isEmpty():
            print(f"No data found for pattern: {file_pattern}. Returning empty DataFrame.")
            return spark.createDataFrame([], schema)

        print(f"Successfully read data with {df.count()} rows.")
        return df
    except Exception as e:
        print(f"Error reading data from {file_pattern}: {e}")
        raise e

def add_metadata(df: DataFrame, table_name: str) -> DataFrame:
    print(f"Adding metadata columns to DataFrame for table: {table_name}")
    df_transformed = df.withColumn("ingestion_timestamp", current_timestamp()) \
                       .withColumn("source_table", lit(table_name))
    print("Metadata columns added.")
    return df_transformed

def write_to_bigquery(df: DataFrame, project_id: str, bq_dataset_id: str, table_name: str):
    if df.count() == 0:
        print(f"DataFrame for {table_name} is empty. Skipping write operation.")
        return

    bq_table_id = f"{bq_dataset_id}.{table_name}"
    print(f"Writing data to BigQuery table: {project_id}.{bq_table_id}")
    (
        df.write.format("bigquery")
        .option("table", bq_table_id)
        .option("project", project_id)
        .mode("overwrite")
        .save()
    )
    print(f"Successfully wrote data to BigQuery table: {bq_table_id}")