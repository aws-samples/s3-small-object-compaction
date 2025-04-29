import datetime
import json
import boto3
from datetime import timedelta, datetime
import os


def get_dates_in_range(duration, date_format):
    start_date = datetime.now() - timedelta(days=duration)
    dates = []
    for n in range(duration):
        date = start_date + timedelta(days=n)
        dates.append(date.strftime(date_format))
    return dates

def ensure_trailing_slash(uri):
    """Ensure the URI ends with a trailing slash"""
    if not uri.endswith('/'):
        return uri + '/'
    return uri

def lambda_handler(event, context):
    print(event)
    
    # Ensure source and destination URIs have trailing slashes
    source_uri = ensure_trailing_slash(event["s3_source_uri"])
    destination_uri = ensure_trailing_slash(event["s3_destination_uri"])
    
    dates = get_dates_in_range(event["duration"], event["date_format"])
    s3_locations = []
    for date in dates:
        print(f"Selected {date} of {len(dates)}")
        location = {
             "src": json.dumps(source_uri + str(date)),
             "dest": json.dumps(destination_uri + str(date))
             }
        s3_locations.append(location)
    print("Prefix list complete!")
    
    # Create JSONL content
    jsonl_content = ""
    for location in s3_locations:
        jsonl_content += json.dumps(location) + "\n"
    
    # Extract bucket name and create S3 client
    bucket_name = destination_uri.replace("s3://", "").split("/")[0]
    
    # Write to S3
    s3_client = boto3.client('s3')
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    s3_client.put_object(
        Body=jsonl_content,
        Bucket=bucket_name,
        Key=f"locations_{timestamp}.jsonl"
    )
    
    print(f"JSONL file uploaded to s3://{bucket_name}/locations_{timestamp}.jsonl")
    
    return {
        "s3_locations_bucket": bucket_name,
        "s3_locations_key": f"locations_{timestamp}.jsonl"
    }