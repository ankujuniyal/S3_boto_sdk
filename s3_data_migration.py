import botocore.config
import requests
import boto3
from boto3.s3.transfer import TransferConfig

# Define source and destination bucket names
source_bucket_name = 'abc'
destination_bucket_name = 'abcz'

proxies = {"http": ''}
session = requests.Session()
session.proxies = proxies

botocore_session = botocore.config.Config(
        proxies=proxies
    )

s3 = boto3.client('s3')
s3_source_bucket = boto3.client('s3',
                                      endpoint_url='',
                                      aws_access_key_id='',
                                      aws_secret_access_key='',
                                      verify=False,
                                      config=botocore_session,
                                      region_name="")


s3_destination_bucket = boto3.client('s3',
                                      endpoint_url='',
                                      aws_access_key_id='',
                                      aws_secret_access_key='',
                                      verify=False,
                                      config=botocore_session,
                                      region_name="")

def list_objects(s3bucket_endpoint,bucket_name):
    objects = []
    try:
        response = s3bucket_endpoint.list_objects_v2(Bucket=bucket_name)
        for obj in response.get('Contents', []):
            objects.append(obj)
    except Exception as e:
        print(f"Error listing objects in {bucket_name}: {e}")
    return objects


def sync_buckets(source_bucket, destination_bucket):
    source_objects = list_objects(s3_source_bucket,source_bucket)
    destination_objects = list_objects(s3_destination_bucket,destination_bucket)

    source_bucket_size = sum(obj['Size'] for obj in source_objects)
    destination_bucket_size = sum(obj['Size'] for obj in destination_objects)
    print(f'source_bucket_size total size: {source_bucket_size // 1000 / 1024 / 1024} GB')
    # print(f'destination_bucket_size total size: {destination_bucket_size // 1000 / 1024 / 1024} GB')

    for source_obj in source_objects:
        key = source_obj['Key']
        current_object = s3_source_bucket.get_object(Bucket=source_bucket_name,Key=key)
        source_etag = source_obj.get('ETag', '').strip('"')
        source_meta_data = s3_source_bucket.head_object(Bucket=source_bucket, Key=key).get('Metadata', {})


        # Check if the object with the same key exists in the destination bucket
        destination_obj = next((obj for obj in destination_objects if obj['Key'] == key), None)

        if destination_obj:
            dest_etag = destination_obj.get('ETag', '').strip('"')

            if dest_etag != source_etag:
                # The source object is newer or has a different size, so copy it
                try:

                    print("updating file")
                    print(key)
                    s3_destination_bucket.upload_fileobj(current_object['Body'], destination_bucket, key,
                                                             ExtraArgs={'Metadata': source_meta_data}, Config=config)

                    print(f"Updated: {key}")
                except Exception as e:
                    print(f"Error copying if size is not equal {key}: {e}")
            else:
                print("same etag file: " + key + " found in both locations")
                print("Source ETAG", source_etag)
                print("Destination ETAG", dest_etag)

        else:
            # The object does not exist in the destination bucket, so copy it
            try:
                print("copying : ")
                print(key)
                s3_destination_bucket.upload_fileobj(current_object['Body'], destination_bucket, key,
                                                     ExtraArgs={'Metadata': source_meta_data}, Config=config)
                print(f"Copied: {key}")
            except Exception as e:
                print(f"Error copying if object not exist {key}: {e}")


if __name__ == "__main__":
    GB = 1024 ** 3
    config = TransferConfig(multipart_threshold=10 * GB)
    sync_buckets(source_bucket_name, destination_bucket_name)
