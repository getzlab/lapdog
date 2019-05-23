from __future__ import print_function
import sys
import subprocess
import time
import os
import tempfile
import requests
import threading
import base64

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


class BatchWriter:
    """
    Better Log batching system
    Dispatches a chunk of log text after a set time or if the buffer reaches a set size
    Both thresholds increase as the total log volume increases
    """

    def __init__(self):
        self.batch_start = None
        self.batch_contents = ''
        self.log_volume = 0
        self.lock = threading.Condition()
        self.thread = threading.Thread(
            target=self._threadworker,
            name="Batch writing thread",
        )
        self.thread.daemon = True
        self.thread.start()

    def write(self, text):
        with self.lock:
            if self.batch_start is None:
                self.batch_start = time.time()
            self.batch_contents += text.rstrip() + '\n'
            if self.dispatch_time() or self.dispatch_volume():
                self.lock.notify_all()

    def dispatch_time(self):
        if self.log_volume > 1073741824:
            wait_time = 1800
        elif self.log_volume > 52428800:
            wait_time = 300
        else:
            wait_time = 60
        return self.batch_start is not None and time.time() - self.batch_start >= wait_time

    def dispatch_volume(self):
        if self.log_volume > 1073741824:
            wait_size = 4096
        elif self.log_volume > 52428800:
            wait_size = 2048
        else:
            wait_size = 1024
        return len(self.batch_contents) >= wait_size

    def _threadworker(self):
        with tempfile.NamedTemporaryFile('w') as tmp:
            while True:
                with self.lock:
                    while not (self.dispatch_time() or self.dispatch_volume()):
                        self.lock.wait(10)
                    tmp.write(self.batch_contents)
                    self.log_volume += (
                        self.log_volume # Yes we want to include this, because the upload re-writes the blob
                        + len(self.batch_contents)
                    )
                    self.batch_contents = ''
                    self.batch_start = None
                tmp.flush()
                subprocess.check_call('gsutil -h "Content-Type:text/plain" cp %s %s' % (tmp.name, sys.argv[1]), shell=True)

writer = BatchWriter()
while True:
    line = raw_input() # Dumb, but will crash when stdin closes
    if line.rstrip() == '<<<EOF>>>':
        sys.exit(0)
    writer.write(line)
    # tmp.write('\n(VOLUME %d)\n' % volume)
    print(line.rstrip())
