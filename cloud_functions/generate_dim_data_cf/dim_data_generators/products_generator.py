import random

class ProductsGenerator:
    def __init__(self, num_records):
        self.num_records = num_records

    def generate_products(self):
        products = []
        for _ in range(self.num_records):
            product = {
                "product_id": random.randint(1, 500),
                "product_name": f"Product_{random.randint(1, 500)}",
                "category": random.choice(["Electronics", "Furniture", "Clothing", "Food"]),
                "price": round(random.uniform(5.5, 100.5), 2)
            }
            products.append(product)
        return products
