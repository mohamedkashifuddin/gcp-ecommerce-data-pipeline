import datetime
import os
import csv
from io import StringIO
from google.cloud import storage

from dim_data_generators import CustomersGenerator, ProductsGenerator, DatesGenerator

storage_client = storage.Client()


def save_to_gcs_csv(data, bucket_name, gcs_path):
    if not data:
        print(f"No data to save for {gcs_path}. Skipping.")
        return

    keys = data[0].keys()
    output_buffer = StringIO()

    writer = csv.DictWriter(output_buffer, fieldnames=keys, lineterminator='\n')
    writer.writeheader()
    writer.writerows(data)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    blob.upload_from_string(output_buffer.getvalue(), content_type='text/csv')
    print(f"Data saved to gs://{bucket_name}/{gcs_path}")


def generate_dim_data_cf(request):
    num_customers = 1000
    num_products = 500
    num_dates = 365
    start_date = datetime.date(2022, 1, 1)
    end_date = datetime.date(2022, 12, 31)

    customers_gen = CustomersGenerator(num_customers)
    products_gen = ProductsGenerator(num_products)
    dates_gen = DatesGenerator(num_dates, start_date, end_date)

    customers_data = customers_gen.generate_customers()
    products_data = products_gen.generate_products()
    dates_data = dates_gen.generate_dates()

    gcs_raw_bucket_name = os.environ.get('GCS_RAW_BUCKET_NAME')
    if not gcs_raw_bucket_name:
        raise ValueError("GCS_RAW_BUCKET_NAME environment variable not set.")

    current_date = datetime.datetime.now().strftime("%Y-%m-%d")

    customers_gcs_path = f'dim/customers_{current_date}.csv'
    products_gcs_path = f'dim/products_{current_date}.csv'
    dates_gcs_path = f'dim/dates_{current_date}.csv'

    save_to_gcs_csv(customers_data, gcs_raw_bucket_name, customers_gcs_path)
    save_to_gcs_csv(products_data, gcs_raw_bucket_name, products_gcs_path)
    save_to_gcs_csv(dates_data, gcs_raw_bucket_name, dates_gcs_path)

    return f"Dimension data generation complete. Files uploaded to gs://{gcs_raw_bucket_name}/dim/"