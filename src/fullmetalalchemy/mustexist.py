from inspect import getmembers


def mustexist(method):
    def wrapper(*args, **kwargs):
        print(dict(getmembers(method)))
        table = dict(getmembers(method))['__self__']
        if not table.exists():
            raise Exception('Table does not exist')
        return method(*args, **kwargs)
    return wrapper