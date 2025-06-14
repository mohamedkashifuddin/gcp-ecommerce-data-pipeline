import random

class OrderItemsGenerator:
    def __init__(self, num_records, order_ids):
        self.num_records = num_records
        self.order_ids = order_ids

    def generate_order_items(self):
        items = []
        dirty_items = []
        used_ids = set()

        for _ in range(self.num_records):
            item_id = random.randint(10000, 99999)
            while item_id in used_ids:
                item_id = random.randint(10000, 99999)
            used_ids.add(item_id)

            order_id = random.choice(self.order_ids)
            product_id = random.randint(1, 500)
            quantity = random.choice([random.randint(1, 10), -3])  # inject dirty
            price = round(random.uniform(5.5, 100.5), 2)

            record = {
                "order_item_id": item_id,
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "price": price
            }

            if quantity <= 0:
                dirty_items.append(record)
            else:
                items.append(record)

        return items, dirty_items
