from functools import wraps, partial
import s3
import dynamodb
import hashlib
import uuid
import time
import math


def getUnix():
    return math.floor(time.time())


def hashMD5(*tup):
    st = ':'.join([str(t) for t in tup])
    return hashlib.md5(st.encode('utf-8')).hexdigest()


def findReferenceByPk(pk):
    key_expression = 'pk = :a'
    expression_value = {':a': {'S': pk}}
    return dynamodb.query(key_expression, expression_value)


def cache(func=None, **kargs):

    if not func:
        return partial(cache, **kargs)

    @wraps(func)
    def wrapper(*iargs, **ikwargs):
        pk = f'{func.__name__}-{hashMD5(*iargs)}'
        refs = findReferenceByPk(pk)
        bucket = 'hellovictorlee-cache'
        if refs:
            keys = []
            for ref in refs:
                keys = keys + ref['keys']
            # TODO: keys => batch get
            return s3.get(bucket=bucket, key=keys[0])
        body = func(*iargs, **ikwargs)
        key = f'{func.__name__}/{uuid.uuid4().hex}.json'
        s3.put(body=body, bucket=bucket, key=key)
        dynamodb.put({'pk': pk, 'sk': f'{getUnix()}', 'keys': [key]})
        return body

    return wrapper


if __name__ == '__main__':

    @cache
    def hello(a, b):
        return {'a': a, 'b': b}

    print(hello('hi', 'there'))
