import random
import datetime

class DatesGenerator:
    def __init__(self, num_records, start_date, end_date):
        self.num_records = num_records
        self.start_date = start_date
        self.end_date = end_date

    def generate_dates(self):
        dates = []
        for _ in range(self.num_records):
            random_date = self.start_date + datetime.timedelta(days=random.randint(0, (self.end_date - self.start_date).days))
            date = {
                "date_id": random_date.strftime("%Y%m%d"),
                "date": random_date.strftime("%Y-%m-%d"),
                "day_of_week": random_date.strftime("%A"),
                "month": random_date.strftime("%B"),
                "year": random_date.year
            }
            dates.append(date)
        return dates
