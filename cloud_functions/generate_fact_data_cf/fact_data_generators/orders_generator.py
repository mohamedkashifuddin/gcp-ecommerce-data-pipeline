import random
import datetime

class OrdersGenerator:
    def __init__(self, num_records, start_date, end_date):
        self.num_records = num_records
        self.start_date = start_date
        self.end_date = end_date

    def generate_orders(self):
        orders = []
        dirty_orders = []
        used_ids = set()

        for _ in range(self.num_records):
            order_id = random.randint(1000, 9999)
            while order_id in used_ids:
                order_id = random.randint(1000, 9999)
            used_ids.add(order_id)

            order_date = self.start_date + datetime.timedelta(days=random.randint(0, (self.end_date - self.start_date).days))
            customer_id = random.choice([random.randint(1, 1000), None])  # inject missing customer_id
            total_amount = random.choice([round(random.uniform(20.5, 500.5), 2), -10.0])  # inject negative amount

            record = {
                "order_id": order_id,
                "order_date": str(order_date),
                "customer_id": customer_id,
                "total_amount": total_amount
            }

            if customer_id is None or total_amount < 0:
                dirty_orders.append(record)
            else:
                orders.append(record)

        return orders, dirty_orders
