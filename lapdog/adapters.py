from google.cloud import storage
import subprocess
import yaml
from io import StringIO
import json
import time
import threading
import os
from functools import lru_cache
import contextlib
import re

# individual VMs can be tracked viua a combination of labels and workflow meta
# Workflows are reported by the cromwell_driver output and then VMs can be tracked
# A combination of the Workflow label and the call- label should be able to uniquely identify tasks
# Scatter jerbs will have colliding VMs, but can that will have to be handled elsewhere
# Possibly, the task start pattern will contain multiple operation starts (or maybe shard-ids are given as one of the digit fields)

workflow_start_pattern = re.compile(r'WorkflowManagerActor Starting workflow UUID\(([a-z0-9\-]+)\)')
task_start_pattern = re.compile(r'\[UUID\((\w{8})\)(\w+)\.(\w+):(\w+):(\d+)\]: job id: (operations/[a-z0-9\-])')
msg_pattern = re.compile(r'UUID\((\w{8})\)')
fail_pattern = re.compile(r"ERROR - WorkflowManagerActor Workflow ([a-z0-9\-]+) failed \(during *?\): Job (\w+)\.(\w+):\w+:\d+ (.+)")

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
    def __init__(self, cmd, *args, **kwargs):
        r,w = os.pipe()
        r2,w2 = os.pipe()
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

class SubmissionAdapter(object):
    def __init__(self, gs_path):
        try:
            data = json.loads(getblob(gs_path).download_as_string())
        except:
            raise FileNotFoundError("Unable to locate the submission file " + gs_path)
        self.workspace = data['workspace']
        self.namespace = data['namespace']
        self.identifier = data['identifier']
        self.operation = data['operation']
        self.thread = None
        self.workflows = {}

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

    def read_cromwell(self):
        """
        Returns a file-object which reads stdout from the submission Cromwell VM
        """
        status = self.status # maybe this shouldn't be a property...it takes a while to load
        cmd = (
            "gcloud compute ssh --zone {zone} {instance_name} -- "
            "'docker logs -f $(docker ps -q)'"
        ).format(
            zone=status['metadata']['runtimeMetadata']['computeEngine']['zone'],
            instance_name=status['metadata']['runtimeMetadata']['computeEngine']['instanceName']
        )
        return CommandReader(cmd, shell=True, executable='/bin/bash')

    def get_workflows(self, workflows):
        # This, at the very least, informs the adapter how many workflows to expect
        # The workflow_start_pattern will inform cromwell of when each workflow checks in
        # Depending on the data available in the status_json we may or may not be able to
        # Link workflows at this point
        # Alternatively, we can sniff the logs in from the workflow itself
        # Input keys are derived entirely from values, not variable names, so we
        #   might have enough data to link workflows that way

        # This should start a monitoring thread to watch for patterns in the cromwell logs
        # Then, relevant information is logged to this object or child workflow Adapters
        pass
