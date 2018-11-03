import os
import time
import shutil
import time
from functools import lru_cache, partial, wraps

CACHES = {}

def cached(timeout, cache_size=4):

    def wrapper(func):

        @lru_cache(cache_size)
        def cachefunc(*args, **kwargs):
            # print("Cache expired. Running", func)
            return (time.time(), func(*args, **kwargs))

        def call_func(*args, **kwargs):
            result = cachefunc(*args, **kwargs)
            if time.time() - result[0] > timeout:
                cachefunc.cache_clear()
                result = cachefunc(*args, **kwargs)
            # else:
                # print("Cache intact. Retrieving cached results from", func)
            return result[1]

        call_func.cache_clear = cachefunc.cache_clear

        return call_func

    return wrapper

def cache_type(key):
    def wrapper(func):
        CACHES[key] = func
        return func
    return wrapper

def cache_init():
    if 'LAPDOG_CACHE' in os.environ:
        path = os.environ['LAPDOG_CACHE']
    else:
        path = os.path.expanduser(os.path.join(
            '~',
            '.cache',
            'lapdog'
        ))
    if not os.path.isdir(path):
        os.makedirs(path)
    return path

@cache_type('submission')
def _submission_type(namespace, workspace, submission_id, dtype, ext):
    return os.path.join(
        cache_init(),
        'submissions.%s.%s.%s.%s%s' % (
            namespace, workspace, submission_id, dtype, ext
        )
    )

@cache_type('workflow')
def _workflow_type(submission_id, workflow_id, dtype, ext):
    return os.path.join(
        cache_init(),
        'workflows.%s.%s.%s%s' % (
            submission_id, workflow_id, dtype, ext
        )
    )

def _default_type(*args, **kwargs):
    return os.path.join(
        cache_init(),
        'cachedata.kw.' + '.'.join(key+'-'+str(val) for key, val in kwargs.items()) + '.pos.' + '.'.join(args)
    )

@cache_type('operation')
def _operation_type(operation_id, dtype, ext):
    return os.path.join(
        cache_init(),
        'operations.%s' % operation_id.replace('operations/', '')
    )

@cache_type('submission-json')
def _json_type(bucket_id, submission_id, dtype, ext):
    return os.path.join(
        cache_init(),
        'submission-json.%s.%s.%s%s' % (
            bucket_id, submission_id, dtype, ext
        )
    )

def cache_path(key):
    if key in CACHES:
        return CACHES[key]
    return _default_type


def cache_fetch(object_type, *args, dtype='data', ext='', decode=True, **kwargs):
    if len(ext) and not ext.startswith('.'):
        ext = '.' + ext
    decode = 'r' if decode else 'rb'
    path = cache_path(object_type)(*args, dtype=dtype, ext=ext, **kwargs)
    if os.path.isfile(path):
        if time.time() - os.stat(path).st_mtime > 2628001: # Expires after 1 month
            os.remove(path)
            return None
        # print("<CACHE> Read data from", path)
        with open(path, decode) as r:
            return r.read()
    return None

def cache_write(data, object_type, *args, dtype='data', ext='', decode=True, **kwargs):
    args = [str(arg).replace('/', '_') for arg in args]
    kwargs = {k:str(v).replace('/', '_' ) for k,v in kwargs.items()}
    if len(ext) and not ext.startswith('.'):
        ext = '.' + ext
    path = cache_path(object_type)(*args, dtype, ext, **kwargs)
    # print("<CACHE> Write data to", path)
    if decode:
        data = str(data)
    decode = 'w' if decode else 'wb'
    with open(path, decode) as w:
        w.write(data)
