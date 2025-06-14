import csv
from io import StringIO
from google.cloud import storage

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