import logging
import boto3
from botocore.exceptions import ClientError
from contextlib import closing
import uuid
import json


def create_bucket(bucket, region=None, ACL='private'):
    try:
        if region:
            s3 = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3.create_bucket(Bucket=bucket, CreateBucketConfiguration=location)
        else:
            s3 = boto3.client('s3')
            location = {'LocationConstraint': region}
            s3.create_bucket(Bucket=bucket)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def list_buckets():
    try:
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        return [bucket["Name"] for bucket in response['Buckets']]
    except ClientError as e:
        logging.error(e)
        return None


def put(body, bucket, key):
    s3 = boto3.client('s3')
    try:
        return s3.put_object(Body=json.dumps(body), Bucket=bucket, Key=key)
    except ClientError as e:
        logging.error(e)
        return None


def get(bucket, key):
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body']
        with closing(body) as stream:
            data = ''.join([s.decode("utf-8") for s in stream])
            return json.loads(data)
    except ClientError as e:
        logging.error(e)
        return None


if __name__ == '__main__':
    bucket = 'hellovictorlee-cache'
    region = 'us-west-2'

    # create_bucket(bucket=bucket, region=region)
    # lst = list_buckets()
    # print(lst)

    # data = {'hello': 'world'}
    # key = f'{str(uuid.uuid4())}.json'
    # put(data, bucket, key)

    # res = get(bucket=bucket, key='a6b37cf1-2410-4cab-b40f-ee56eaf6df01.json')
    # print(res)
