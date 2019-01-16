from __future__ import print_function
import sys
import subprocess
import time
import os
import tempfile
import requests
import threading
import base64

last_call = None

abort_path = os.path.join(
    os.path.dirname(os.path.dirname(sys.argv[1])),
    'abort-key'
)

def abort():

    if not os.path.exists(abort_path):
        subprocess.check_call('gsutil cp %s abort.key' % abort_path, shell=True)
    print('<<<ABORT KEY DETECTED>>>')
    with open('abort.key', 'rb') as r:
        response = requests.post(
            os.environ.get("SIGNATURE_ENDPOINT"),
            headers={
                'Content-Type': 'application/json'
            },
            json={
                'key': base64.b64encode(r.read()),
                'id': os.environ.get("LAPDOG_SUBMISSION_ID")
            }
        )
    if response.status_code == 200:
        response = requests.get('http://localhost:8000/api/workflows/v1/query?status=Running')
        print("(%d) : %s" % (response.status_code, response.text))
        for workflow in response.json()['results']:
            print("WORKFLOW:", workflow)
            print(requests.post('http://localhost:8000/api/workflows/v1/%s/abort' % workflow['id']).text)
        # for workflow in requests.get('http://localhost:8000/api/workflows/v1/query?status=Running').json():
        #     print(requests.post('http://localhost:8000/api/workflows/v1/%s/abort' % workflow['id']).text)
        print("<<<ABORTED ALL WORKFLOWS. WAITING FOR CROMWELL SHUTDOWN>>>")
        # subprocess.check_call('gsutil cp %s %s' % (handle.name, sys.argv[1]), shell=True)
        # sys.exit("Aborted")
    else:
        print('<<<INVALID ABORT KEY>>>')


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

def _flush(handle):
    global volume
    handle.flush()
    subprocess.check_call('gsutil -h "Content-Type:text/plain" cp %s %s' % (handle.name, sys.argv[1]), shell=True)
    volume += os.path.getsize(handle.name)

def flush(handle, debounce_interval):
    global last_call
    call_time = time.time()
    last_call = call_time
    n = debounce_interval / 2
    for i in range(n):
        time.sleep(2)
        if last_call != call_time:
            return
    if last_call == call_time:
        _flush(handle)

# flush = throttle(120)(_flush)
# flush_slow = throttle(300)(_flush)
# flush_rare = throttle(3600)(_flush)

def write(handle, line):
    handle.write(line.rstrip() + '\n')
    while threading.activeCount() >= 1000:
        time.sleep(1)
    thread = threading.Thread(
        target=flush,
        args=(
            handle,
            1800 if volume > 1073741824 else (
                300 if volume > 52428800 else 60
            )
        )
    ) # Debounced log thread
    thread.daemon = True
    thread.start()
    # if volume > 1073741824: # 1 Gib
    #     upload = flush_rare(handle)
    # elif volume > 52428800: # 50 Mib
    #     upload = flush_slow(handle)
    # else:
    #     upload = flush(handle)

def abort_worker():
    while True:
        for i in range(60):
            time.sleep(2)
        print("CHECKING ABORT KEY")
        if subprocess.call('gsutil ls %s' % abort_path, shell=True) == 0:
            # abort key located
            print("ABORTING")
            abort()

thread = threading.Thread(
    target=abort_worker
) # abort thread
thread.daemon = True
thread.start()

volume = 0
with tempfile.NamedTemporaryFile('w') as tmp:
    while True:
        line = raw_input() # Dumb, but will crash when stdin closes
        if line.rstrip() == '<<<EOF>>>':
            sys.exit(0)
        write(tmp, line)
        # tmp.write('\n(VOLUME %d)\n' % volume)
        print(line.rstrip())
