import boto3 
import pathlib
import json

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

def split_s3_parts(s3_uri):
    path_parts=s3_uri.replace("s3://","").split("/")
    bucket=path_parts.pop(0)
    key="/".join(path_parts)
    return bucket, key

def lambda_handler(event, context):
    print(event)

    s_bucket, s_key = split_s3_parts(json.loads(event['src']))
    d_bucket, d_key = split_s3_parts(json.loads(event['dest']))

    merge_objects_from_s3(s_bucket, s_key, d_bucket, d_key, "/tmp/")

    print("Compaction complete!")

    return {
        "statusCode": 200,
        "body": "Compaction complete!"
    }