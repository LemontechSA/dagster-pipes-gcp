import polars as pl
from faker import Faker


def get_fake_data(N: int = 100) -> pl.DataFrame:
    data = []
    fake = Faker()
    for _ in range(100):
        data.append(
            {
                "name": fake.name(),
                "address": fake.address(),
                "email": fake.email(),
                "phone": fake.phone_number(),
            }
        )
    return pl.DataFrame(data)
