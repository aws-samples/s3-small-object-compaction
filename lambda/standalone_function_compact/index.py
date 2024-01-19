import datetime
import boto3 
import pathlib
from datetime import timedelta, datetime

s3 = boto3.client('s3')

def list_objects_in_s3(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if response.get('Contents'):
        return response['Contents']
    else:
        return 'None'

def get_object_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()

def upload_object_to_s3(bucket, key, file_path):
    s3.upload_file(file_path, bucket, key)

def merge_objects_from_s3(source_bucket, source_prefix, target_bucket, target_prefix, temp_path):
    objects = list_objects_in_s3(source_bucket, source_prefix)
    if objects == 'None':
        return
    else:
        out_path = temp_path + target_prefix.replace("/", "-") + objects[0]['Key'].split("/")[-1] + ''.join(pathlib.Path(objects[0]['Key'].split("/")[-1]).suffixes)
        out_key = target_prefix + "/" + target_prefix.replace("/", "-") +  ''.join(pathlib.Path(objects[0]['Key'].split("/")[-1]).suffixes)
        for object in objects:
            key = object['Key']
            data = get_object_from_s3(source_bucket, key)
            with open(out_path, 'ab') as f:
                f.write(data)
        upload_object_to_s3(target_bucket, out_key, out_path)
        print(f"Merged {len(objects)} objects into {out_key}")

def get_dates_in_range(duration, date_format):
    start_date = datetime.now() - timedelta(days=duration)
    dates = []
    for n in range(duration):
        date = start_date + timedelta(days=n)
        dates.append(date.strftime(date_format))
    return dates

def split_s3_parts(s3_uri):
    path_parts=s3_uri.replace("s3://","").split("/")
    bucket=path_parts.pop(0)
    key="/".join(path_parts)
    return bucket, key

def handler(event, context):
    print(event)

    s_bucket, s_key = split_s3_parts(event['s3_source_uri'])
    d_bucket, d_key = split_s3_parts(event['s3_destination_uri'])

    dates = get_dates_in_range(event['duration'], event['date_format'])

    for date in dates:
        merge_objects_from_s3(s_bucket, s_key + date, d_bucket, d_key + date, "/tmp/")
        print(f"Compacted {date} of {len(dates)}")
    print("Compaction complete!")
    return {
        "statusCode": 200,
        "body": "Compaction complete!"
    }