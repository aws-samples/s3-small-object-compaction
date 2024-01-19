
import random
import boto3
import os

s3 = boto3.client('s3')

def generate_test_data(rows_per_file, files, bucket_name):
    first_names = ["John", "Jane", "Bob", "Alice"]
    last_names = ["Smith", "Johnson", "Brown", "Davis"]
    addresses = ["123 Main St", "456 Park Ave", "789 Elm St", "321 Oak St"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston"]
    zip_codes = ["10001", "10002", "10003", "10004"]

    for i in range(files):
        file_name = f"test_data_{i}.json"
        random_date_prefix = get_prefix()
        with open(file_name, "w") as f:
            for j in range(rows_per_file):
                record = {
                    "first_name": random.choice(first_names),
                    "last_name": random.choice(last_names),
                    "age": random.randint(18, 65),
                    "address": random.choice(addresses),
                    "city": random.choice(cities),
                    "zip_code": random.choice(zip_codes),
                    "source_date": random_date_prefix,
                    }
                f.write(f"{record}\n")
        print(f"{rows_per_file} lines written to {file_name}")
        upload_file_to_s3(file_name, bucket_name, random_date_prefix)
        print(f"{file_name} uploaded to S3")
        os.remove(file_name)
        print(f"{file_name} removed")   

def get_prefix():
    month = random.choice(["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"])
    day = random.choice(["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"])
    random_date_prefix = "2023/" + month + "/" + day + "/"
    return random_date_prefix

def upload_file_to_s3(file_path, bucket_name, random_date_prefix):
    s3.upload_file(file_path, bucket_name, random_date_prefix + file_path)

def main():
    rows_per_file = 1000
    files = 10000
    bucket_name = "<<you-bucket-name>>"
    print(f"Generating {files} files with {rows_per_file} lines each")
    generate_test_data(rows_per_file, files, bucket_name)
    print("Test data generated successfully")

if __name__ == "__main__":
    main()