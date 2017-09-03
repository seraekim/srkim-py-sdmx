import inspect
import logging as log

log.basicConfig(format='[%(levelname)-5s] %(asctime)s.%(msecs)03d File "%(pathname)s", line %(lineno)d, '
                       'in %(funcName)s, %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                level=log.DEBUG)


def get_class_name():
    st = inspect.stack()
    return str(st[len(st)-1][4][0])

