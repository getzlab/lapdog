import os
import time
import shutil
import time
from functools import lru_cache, partial, wraps
from hashlib import md5

CACHES = {}

def cached(timeout, cache_size=4):
    """
    Wrapper to apply a time-based cache to the decorated function.
    Specify a cache timeout in seconds. When an entry is recalled which is older than
    the timeout, the cache is emptied. Currently single entries cannot be invalidated.
    Set `cache_size` to specify the maximum size of the cache. If the cache exceeds
    the size limit while still within the timeout, the least recently used (LRU)
    cached value is removed to make room for the new entry
    """

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

def path_eval(func):

    def call(*args, **kwargs):
        path = func(*args, **kwargs)
        key = md5(path.encode()).hexdigest()
        dirpath = os.path.join(
            cache_init(),
            key[:2],
            key[2:4]
        )
        os.makedirs(dirpath, exist_ok=True)
        return os.path.join(dirpath, path)

    return call

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
@path_eval
def _submission_type(namespace, workspace, submission_id, dtype, ext):
    return 'submissions.%s.%s.%s.%s%s' % (
        namespace, workspace, submission_id, dtype, ext
    )

@cache_type('workflow')
@path_eval
def _workflow_type(submission_id, workflow_id, dtype, ext):
    return 'workflows.%s.%s.%s%s' % (
        submission_id, workflow_id, dtype, ext
    )

@path_eval
def _default_type(*args, **kwargs):
    return 'cachedata.kw.' + '.'.join(key+'-'+str(val) for key, val in kwargs.items()) + '.pos.' + '.'.join(args)

@cache_type('operation')
@path_eval
def _operation_type(operation_id, dtype, ext):
    return 'operations.%s' % operation_id.replace('operations/', '')

@cache_type('submission-json')
@path_eval
def _json_type(bucket_id, submission_id, dtype, ext):
    return 'submission-json.%s.%s.%s%s' % (
        bucket_id, submission_id, dtype, ext
    )

@cache_type('submission-config')
@path_eval
def _config_type(bucket_id, submission_id, dtype, ext):
    return 'config-tsv.%s.%s.%s%s' % (
        bucket_id, submission_id, dtype, ext
    )

@cache_type('submission-pointer')
@path_eval
def _pointer_type(bucket_id, submission_id, dtype='data', ext=''):
    return 'submission-ptr.%s.%s.%s%s' % (
        bucket_id, submission_id, dtype, ext
    )

def cache_path(key):
    if key in CACHES:
        return CACHES[key]
    return _default_type


def cache_fetch(object_type, *args, dtype='data', ext='', decode=True, **kwargs):
    """
    Fetches a value from the offline disk cache.
    `object_type` specifies which handler will be used to translate remaining arguments
    into a filepath.
    If no handler can be found for a given `object_type`, use a default handler.
    The object type or arguments is generally not important, as long as you remain
    consistent with the order and type of arguments for a given object_type.

    Returns None if the cache entry could not be found
    """
    if len(ext) and not ext.startswith('.'):
        ext = '.' + ext
    decode = 'r' if decode else 'rb'
    path = cache_path(object_type)(*args, dtype=dtype, ext=ext, **kwargs)
    if os.path.isfile(path):
        # if time.time() - os.stat(path).st_mtime > 2628001: # Expires after 1 month
        #     os.remove(path)
        #     return None
        # print("<CACHE> Read data from", path)
        with open(path, decode) as r:
            return r.read()
    return None

def cache_write(data, object_type, *args, dtype='data', ext='', decode=True, **kwargs):
    """
    Writes a value to the offline disk cache.
    `object_type` specifies which handler will be used to translate remaining arguments
    into a filepath.
    If no handler can be found for a given `object_type`, use a default handler.
    The object type or arguments is generally not important, as long as you remain
    consistent with the order and type of arguments for a given object_type.
    """
    args = [str(arg).replace('/', '_') for arg in args]
    kwargs = {k:str(v).replace('/', '_' ) for k,v in kwargs.items()}
    if len(ext) and not ext.startswith('.'):
        ext = '.' + ext
    path = cache_path(object_type)(*args, dtype=dtype, ext=ext, **kwargs)
    # print("<CACHE> Write data to", path)
    if decode:
        data = str(data)
    decode = 'w' if decode else 'wb'
    with open(path, decode) as w:
        w.write(data)
