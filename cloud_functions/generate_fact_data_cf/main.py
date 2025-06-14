import datetime
import os
from fact_data_generators import OrdersGenerator, OrderItemsGenerator, PaymentsGenerator
from utils.gcs_saver import save_to_gcs_csv # Import the GCS saving utility

def generate_fact_data_cf(request):
    # Parameters
    num_orders = 1000
    num_order_items = 3000
    num_payments = 1000
    start_date = datetime.date(2022, 1, 1)
    end_date = datetime.date(2022, 12, 31)
    today = datetime.date.today().strftime('%Y-%m-%d')

    gcs_raw_bucket_name = os.environ.get('GCS_RAW_BUCKET_NAME')
    if not gcs_raw_bucket_name:
        raise ValueError("GCS_RAW_BUCKET_NAME environment variable not set.")

    # Orders
    orders_gen = OrdersGenerator(num_orders, start_date, end_date)
    orders, dirty_orders = orders_gen.generate_orders()
    save_to_gcs_csv(orders, gcs_raw_bucket_name, f"fact/orders_{today}.csv")
    save_to_gcs_csv(dirty_orders, gcs_raw_bucket_name, f"fact/orders_dirty_{today}.csv")

    # Order Items
    order_ids = [o["order_id"] for o in orders]
    order_items_gen = OrderItemsGenerator(num_order_items, order_ids)
    order_items, dirty_items = order_items_gen.generate_order_items()
    save_to_gcs_csv(order_items, gcs_raw_bucket_name, f"fact/order_items_{today}.csv")
    save_to_gcs_csv(dirty_items, gcs_raw_bucket_name, f"fact/order_items_dirty_{today}.csv")

    # Payments
    payments_gen = PaymentsGenerator(num_payments, order_ids)
    payments, dirty_payments = payments_gen.generate_payments()
    save_to_gcs_csv(payments, gcs_raw_bucket_name, f"fact/payments_{today}.csv")
    save_to_gcs_csv(dirty_payments, gcs_raw_bucket_name, f"fact/payments_dirty_{today}.csv")

    print(" Dummy + Dirty data generated successfully and uploaded to GCS.")
    return f"Fact data generation complete. Files uploaded to gs://{gcs_raw_bucket_name}/fact/"