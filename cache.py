from functools import wraps, partial


def cache(func=None, **kargs):
    if not func:
        return partial(cache, **kargs)

    @wraps(func)
    def wrapper(*iargs, **ikwargs):
        print('*****:', kargs)
        return func(*iargs, **ikwargs)

    return wrapper


if __name__ == '__main__':

    @cache(ia='hi', ib='there')
    def hello(a, b):
        print('a, b:', a, b)

    hello('hi', 'there')
