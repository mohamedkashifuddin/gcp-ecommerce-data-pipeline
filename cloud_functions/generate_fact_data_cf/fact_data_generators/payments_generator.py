import random
import datetime

class PaymentsGenerator:
    def __init__(self, num_records, order_ids):
        self.num_records = num_records
        self.order_ids = order_ids

    def generate_payments(self):
        payments = []
        dirty_payments = []
        used_ids = set()

        for _ in range(self.num_records):
            payment_id = random.randint(100000, 999999)
            while payment_id in used_ids:
                payment_id = random.randint(100000, 999999)
            used_ids.add(payment_id)

            order_id = random.choice(self.order_ids)
            payment_date = str(datetime.date.today())
            payment_amount = random.choice([round(random.uniform(20.5, 500.5), 2), -25.0])  # dirty

            record = {
                "payment_id": payment_id,
                "order_id": order_id,
                "payment_date": payment_date,
                "payment_amount": payment_amount
            }

            if payment_amount <= 0:
                dirty_payments.append(record)
            else:
                payments.append(record)

        return payments, dirty_payments
