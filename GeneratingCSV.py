import csv
from faker import Faker
import random
import os

fake = Faker()

filename = "5GB_fake_data.csv"

target_size_bytes = 5 * 2 ** 30  # 5 GB

header = ["Name","Age", "Gender", "Email", "Address", "Bio"]

def generate_row():
    name = fake.name()
    age = random.randint(18, 80)
    gender = random.choice(["male", "female"])
    email = fake.email()
    address = fake.address().replace("\n", ", ")
    bio = " ".join(fake.sentences(nb=random.randint(20, 50)))
    return [name, age, gender, email, address, bio]

# Stream writing to CSV
with open(filename, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(header)

    current_size = os.path.getsize(filename)
    row_count = 0

    while current_size < target_size_bytes:
        row = generate_row()
        writer.writerow(row)
        row_count += 1

        if row_count % 10000 == 0:
            current_size = os.path.getsize(filename)
            print(f"{row_count} rows written, current file size: {current_size / (1024*1024):.2f} MB")

print(f"Finished! {row_count} rows written to {filename}")