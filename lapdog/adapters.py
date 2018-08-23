from google.cloud import storage
import subprocess
import yaml
from io import StringIO, BytesIO
import json
import time
import datetime
import threading
import os
from functools import lru_cache
import contextlib
import re
import select

timestamp_format = '%Y-%m-%dT%H:%M:%SZ'

def sleep_until(dt):
    sleep_time = (datetime.datetime.now() - dt).total_seconds()
    if sleep_time > 0:
        time.sleep(sleep_time)

# individual VMs can be tracked viua a combination of labels and workflow meta
# Workflows are reported by the cromwell_driver output and then VMs can be tracked
# A combination of the Workflow label and the call- label should be able to uniquely identify tasks
# Scatter jerbs will have colliding VMs, but can that will have to be handled elsewhere
# Possibly, the task start pattern will contain multiple operation starts (or maybe shard-ids are given as one of the digit fields)

workflow_start_pattern = re.compile(r'WorkflowManagerActor Starting workflow UUID\(([a-z0-9\-]+)\)')
task_start_pattern = re.compile(r'\[UUID\((\w{8})\)(\w+)\.(\w+):(\w+):(\d+)\]: job id: (operations/[a-z0-9\-])')
msg_pattern = re.compile(r'UUID\((\w{8})\)')
fail_pattern = re.compile(r"ERROR - WorkflowManagerActor Workflow ([a-z0-9\-]+) failed \(during *?\): Job (\w+)\.(\w+):\w+:\d+ (.+)")

class Recall(object):
    value = None
    def apply(self, value):
        self.value = value
        return value

def safe_getblob(gs_path):
    blob = getblob(gs_path)
    if not blob.exists():
        raise FileNotFoundError("No such blob: "+gs_path)
    return blob

def getblob(gs_path):
    bucket_id = gs_path[5:].split('/')[0]
    bucket_path = '/'.join(gs_path[5:].split('/')[1:])
    return storage.Blob(
        bucket_path,
        storage.Client().get_bucket(bucket_id)
    )


def get_operation_status(opid):
    return yaml.load(
        StringIO(
            subprocess.run(
                'gcloud alpha genomics operations describe %s' % (
                    opid
                ),
                shell=True,
                executable='/bin/bash',
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            ).stdout.decode()
        )
    )

class CommandReader(object):
    def __init__(self, cmd, *args, __insert_text=None, **kwargs):
        r,w = os.pipe()
        r2,w2 = os.pipe()
        if __insert_text is not None:
            w.write(__insert_text)
        self.proc = subprocess.Popen(cmd, *args, stdout=w, stderr=w, stdin=r2, universal_newlines=False, **kwargs)
        self.reader = open(r, 'rb')

    def close(self, *args, **kwargs):
        self.reader.close(*args, **kwargs)
        if self.proc.returncode is None:
            self.proc.kill()

    def __getattr__(self, attr):
        if hasattr(self.reader, attr):
            return getattr(self.reader, attr)
        raise AttributeError("No such attribute '%s'" % attr)

## TODO:
## 1) Controller:
##      Cache a cromwell reader and text from the most recent submission
##      On submission change, close the reader and reset text
##      On fetch: use select to check for input (50ms timeout) then read all
##      Optional line_offset input returns that line and all after
## 2) Adapter:

class SubmissionAdapter(object):
    def __init__(self, bucket, submission):
        gs_path = os.path.join(
            'gs://'+bucket,
            'lapdog-executions',
            submission,
            'submission.json'
        )
        data = json.loads(safe_getblob(gs_path).download_as_string())
        self.workspace = data['workspace']
        self.namespace = data['namespace']
        self.identifier = data['identifier']
        self.operation = data['operation']
        self.thread = None
        self.bucket = bucket
        self.submission = submission
        self.workflows = {}
        self._internal_reader = None

    def update(self):
        if not self.live:
            return
        if self._internal_reader is None:
            self._internal_reader = self.read_cromwell()
        event_stream = []
        while len(select.select([self._internal_reader], [], [], 1)[0]):
            message = self._internal_reader.readline().decode().strip()
            matcher = Recall()
            if matcher.apply(workflow_start_pattern.search(message)):
                print("New workflow started")



    def _parser_thread(self):
        """A thread to continuously eat shit"""
        reader = self.read_cromwell()
        while True:
            line = reader.readline().decode()
            try:
                reader.proc.wait(2)
                return
            except subprocess.TimeoutExpired:
                pass

    @property
    def status(self):
        """
        Get the operation status
        """
        return get_operation_status(self.operation)

    @property
    def live(self):
        """
        Reports if the submission is active or not
        """
        status = self.status
        return 'done' in status and status['done']

    def read_cromwell(self):
        """
        Returns a file-object which reads stdout from the submission Cromwell VM
        """
        status = self.status # maybe this shouldn't be a property...it takes a while to load
        while 'metadata' not in status or 'startTime' not in status['metadata']:
            status = self.status
            time.sleep(1)
        sleep_until(status['metadata']['startTime'] + datetime.datetime.timedelta(seconds=45))
        stdout_blob = getblob(os.path.join(
            'gs://'+self.bucket,
            'lapdog-executions',
            self.submission,
            'logs',
            self.operation[11:]+'-stdout.log'
        ))
        log_text = b''
        if stdout_blob.exists():
            stderr_blob = getblob(os.path.join(
                'gs://'+self.bucket,
                'lapdog-executions',
                self.submission,
                'logs',
                self.operation[11:]+'-stderr.log'
            ))
            log_text = stdout_blob.download_as_string() + (
                stderr_blob.download_as_string() if stderr_blob.exists()
                else b''
            )
        if not 'done' in status and status['done']:
            cmd = (
                "gcloud compute ssh --zone {zone} {instance_name} -- "
                "'docker logs -f $(docker ps -q)'"
            ).format(
                zone=status['metadata']['runtimeMetadata']['computeEngine']['zone'],
                instance_name=status['metadata']['runtimeMetadata']['computeEngine']['instanceName']
            )
            return CommandReader(cmd, __insert_text=log_text, shell=True, executable='/bin/bash')
        else:
            return BytesIO(log_text)

    # def get_workflows(self, workflows):
    #
    #     #LINK via ordering. we know the order in which workflows were submitted
    #     #And so we should know the order in which they are returned
    #     # This, at the very least, informs the adapter how many workflows to expect
    #     # The workflow_start_pattern will inform cromwell of when each workflow checks in
    #     # Depending on the data available in the status_json we may or may not be able to
    #     # Link workflows at this point
    #     # Alternatively, we can sniff the logs in from the workflow itself
    #     # Input keys are derived entirely from values, not variable names, so we
    #     #   might have enough data to link workflows that way
    #
    #     # This should start a monitoring thread to watch for patterns in the cromwell logs
    #     # Then, relevant information is logged to this object or child workflow Adapters
    #     pass


class WorkflowAdapter(object):
    # this adapter needs to be initialized from an input key and a cromwell workflow id (short or long)
    # At first, dispatching events fills the replay buffer
    # when the workflow is started, the buffer is played and the workflow updates to current state DAWG

    def __init__(self, input_key, short_id, long_id=None):
        self.id = short_id
        self.key = input_key
        self.long_id = self.long_id
        self.replay_buffer = []
        self.started = False

    def handle(self, evt, *args, **kwargs):
        if not self.started:
            self.replay_buffer.append((evt, args, kwargs))
            return
        attribute = 'on_'+evt
        if hasattr(self, attribute):
            getattr(self, attribute)(*args, **kwargs)
        print("No handler for event", evt)

    def on_start(self, long_id):
        self.started = True
        self.long_id = long_id
        for event, args, kwargs in self.replay_buffer:
            print("Replaying previous events...")
            self.handle(event, *args, **kwargs)


# wf = WFAdapter(input_key, short_id, long_id=None)
# wf.handle(event)
# ...
# wf.handle(start_event)
# |
# |_ [self.handle(event) for event in self.replay_buffer]
