import boto3
from faker import Faker
import random
import json
import uuid

# Initialize Faker
fake = Faker()

def generate_fake_customer_data():
    customer_data = {
        "customerid": str(uuid.uuid4()),
        "name": fake.name(),
        "address": fake.address(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "signup_date": fake.date_this_decade().isoformat(),
        "loyalty_points": random.randint(0, 1000)
    }
    return customer_data

def generate_fake_order_data():
    op = random.choice(['I', 'U', 'D'])
    replicadmstimestamp = fake.date_time_this_year()
    invoiceid = fake.unique.random_number(digits=5)
    itemid = fake.unique.random_number(digits=2)
    category = fake.word()
    price = round(random.uniform(10, 100), 2)
    quantity = random.randint(1, 5)
    orderdate = fake.date_this_decade()
    destinationstate = fake.state_abbr()
    shippingtype = random.choice(['2-Day', '3-Day', 'Standard'])
    referral = fake.word()

    order_data = {
        "Op": op,
        "replicadmstimestamp": replicadmstimestamp.isoformat(),
        "invoiceid": invoiceid,
        "itemid": itemid,
        "category": category,
        "price": price,
        "quantity": quantity,
        "orderdate": orderdate.isoformat(),
        "destinationstate": destinationstate,
        "shippingtype": shippingtype,
        "referral": referral
    }
    
    return order_data

def upload_to_s3(data, bucket, key):
    # Initialize the S3 client
    s3_client = boto3.client('s3')

    # Convert the data to JSON string
    json_data = json.dumps(data, indent=4, default=str)

    # Upload the JSON string to S3
    s3_client.put_object(Bucket=bucket, Key=key, Body=json_data)

    print(f"JSON file uploaded to S3://{bucket}/{key} successfully.")

# Set the number of fake data entries to generate
num_entries = 10

# Specify the S3 bucket
bucket_name = "XXX"

# Generate and upload customer data
for _ in range(num_entries):
    # Generate fake customer data
    customer_data = generate_fake_customer_data()
    
    # Generate a UUID4 filename
    filename = f"customer_{uuid.uuid4()}.json"
    
    # Specify the S3 key format
    key = f"sample_raw/{filename}"
    
    # Upload the generated data to S3
    upload_to_s3(customer_data, bucket_name, key)

# Generate and upload order data
for _ in range(num_entries):
    # Generate fake order data
    order_data = generate_fake_order_data()
    
    # Generate a UUID4 filename
    filename = f"order_{uuid.uuid4()}.json"
    
    # Specify the S3 key format
    key = f"sample_raw/{filename}"
    
    # Upload the generated data to S3
    upload_to_s3(order_data, bucket_name, key)
