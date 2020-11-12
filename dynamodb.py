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
        return dynamodb.put_item(TableName=table, Item=convert(item))
    except ClientError as e:
        logging.error(e)
        return None


def get(key, table='cache', region='us-west-2'):
    try:
        dynamodb = boto3.client('dynamodb', region_name=region)
        data = dynamodb.get_item(TableName=table, Key=convert(key))
        return revert(data['Item'])
    except ClientError as e:
        logging.error(e)
        return None


def convert(item):
    if type(item) == dict:
        mapper = dict()
        for key, val in item.items():
            _type = type(val)
            if _type in (float, int):
                mapper[key] = {'N': val}
            elif _type == str:
                mapper[key] = {'S': val}
            elif _type == list:
                mapper[key] = {'L': convert(val)}
        return mapper
    elif type(item) == list:
        lst = list()
        for val in item:
            _type = type(val)
            if _type in (float, int):
                lst.append({'N': val})
            elif _type == str:
                lst.append({'S': val})
            elif _type == list:
                lst.append({'L': convert(val)})
        return lst


def revert(item):
    if type(item) == dict:
        mapper = dict()
        for key, val in item.items():
            if key in ('S', 'N'):
                return val
            elif key == 'L':
                return revert(val)
            else:
                mapper[key] = revert(val)
        return mapper
    elif type(item) == list:
        lst = list()
        for val in item:
            lst.append(revert(val))
        return lst


if __name__ == '__main__':
    # create_table()

    # epoch = math.floor(time.time())
    # item = {'pk': 'test', 'sk': str(epoch), 'keys': ['hello', 'world']}
    # put(item)

    # key = {
    #     'pk': 'test',
    #     'sk': '1605168923'
    # }
    # res = get(key)
    # print(res)
