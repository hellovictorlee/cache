import logging
import boto3
import math
from botocore.exceptions import ClientError
from contextlib import closing
import time
import uuid
import json


def create_table(table='cache', region='us-west-2'):
    try:
        dynamodb = boto3.client('dynamodb', region_name=region)
        dynamodb.create_table(AttributeDefinitions=[{
            'AttributeName': 'pk',
            'AttributeType': 'S'
        }, {
            'AttributeName': 'sk',
            'AttributeType': 'S'
        }],
                              TableName=table,
                              KeySchema=[{
                                  'AttributeName': 'pk',
                                  'KeyType': 'HASH'
                              }, {
                                  'AttributeName': 'sk',
                                  'KeyType': 'RANGE'
                              }],
                              BillingMode='PAY_PER_REQUEST')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def put(item, table='cache', region='us-west-2'):
    try:
        dynamodb = boto3.client('dynamodb', region_name=region)
        return dynamodb.put_item(TableName=table, Item=item)
    except ClientError as e:
        logging.error(e)
        return None


def get(key, table='cache', region='us-west-2'):
    try:
        dynamodb = boto3.client('dynamodb', region_name=region)
        return dynamodb.get_item(TableName=table, Key=key)
    except ClientError as e:
        logging.error(e)
        return None


if __name__ == '__main__':
    # create_table()

    epoch = math.floor(time.time())
    item = {
        'pk': {
            'S': 'test'
        },
        'sk': {
            'S': str(epoch)
        },
        'keys': {
            'SS': ['hello', 'world']
        }
    }
    put(item)

    # key = {
    #     'pk': {
    #         'S': 'test'
    #     },
    #     'sk': {
    #         'S': str(epoch)
    #     },
    # }
    # res = get(key)
    # print(res)
