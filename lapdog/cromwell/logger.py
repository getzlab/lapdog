import sys
import subprocess
import time
import os
import tempfile

last_call = None

def throttle(n):
    def wrapper(func):
        def call(*args, **kwargs):
            global last_call
            if last_call is None or (time.time() - last_call) > n:
                last_call = time.time()
                return func(*args, **kwargs)
            # Abort
        return call
    return wrapper

@throttle(120)
def flush(handle):
    handle.flush()
    subprocess.check_call('gsutil cp %s %s' % (handle.name, sys.argv[1]), shell=True)
    return os.path.getsize(handle.name)

@throttle(300)
def flush_slow(handle):
    handle.flush()
    subprocess.check_call('gsutil cp %s %s' % (handle.name, sys.argv[1]), shell=True)
    return os.path.getsize(handle.name)

@throttle(3600)
def flush_rare(handle):
    handle.flush()
    subprocess.check_call('gsutil cp %s %s' % (handle.name, sys.argv[1]), shell=True)
    return os.path.getsize(handle.name)

def write(handle, volume, line):
    handle.write(line.rstrip() + '\n')
    if volume > 1073741824: # 1 Gib
        upload = flush_rare(handle)
    elif volume > 52428800: # 50 Mib
        upload = flush_slow(handle)
    else:
        upload = flush(handle)
    return volume + (upload if upload is not None else 0)

volume = 0
with tempfile.NamedTemporaryFile('w') as tmp:
    while True:
        volume = write(tmp, volume, raw_input()) # Dumb, but will crash when stdin closes
