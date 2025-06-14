# customers_generator.py

import random


class CustomersGenerator:
    def __init__(self, num_records):
        self.num_records = num_records

        self.first_names = [
            "Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona", "George", "Hannah", "Isaac", "Julia",
            "Kevin", "Liam", "Mia", "Noah", "Olivia", "Penelope", "Quinn", "Ryan", "Sophia", "Thomas",
            "Ursula", "Victor", "Wendy", "Xavier", "Yara", "Zack", "Avery", "Blake", "Chloe", "Dylan",
            "Eleanor", "Felix", "Grace", "Henry", "Ivy", "Jasper", "Kayla", "Leo", "Maya", "Nathan",
            "Owen", "Paige", "Riley", "Scarlett", "Tyler", "Violet", "William", "Zara", "Adam", "Bella"
        ]

        self.last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
            "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
            "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
            "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
            "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts"
        ]

    def generate_customers(self):
        customers = []
        for i in range(self.num_records):
            first_name = random.choice(self.first_names)
            last_name = random.choice(self.last_names)

            full_customer_name = f"{first_name} {last_name}"

            customer = {
                "customer_id": i + 1,
                "customer_name": full_customer_name,
                "email": f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 99)}@example.com",
                "join_date": f"2022-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
            }
            customers.append(customer)
        return customers