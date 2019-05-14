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
from .cache import cache_fetch, cache_write, cached, cache_path
from dalmatian import getblob, strict_getblob
from .gateway import Gateway
import traceback
import sys
import threading
from hashlib import md5
import warnings
from agutil import ActiveTimeout, TimeoutExceeded, context_lock
import pandas as pd

# Label filter format: labels.(label name)=(label value)

timestamp_formats = (
    '%Y-%m-%dT%H:%M:%SZ',
    '%Y-%m-%dT%H:%MZ'
)
utc_offset = datetime.datetime.fromtimestamp(time.time()) - datetime.datetime.utcfromtimestamp(time.time())

def sleep_until(dt):
    sleep_time = (dt - datetime.datetime.now()).total_seconds()
    if sleep_time > 0:
        time.sleep(sleep_time)

def parse_time(timestamp):
    """
    Returns a datetime.datetime object from a given UTC timestamp.
    Parses timestamps in multiple known GCP timestamp formats
    """
    #2019-02-04T21:27Z
    # no seconds?
    err = None
    for timestamp_format in timestamp_formats:
        try:
            return datetime.datetime.strptime(
                (timestamp.split('.')[0]+'Z').replace('ZZ', 'Z'),
                timestamp_format
            )
        except ValueError as e:
            err = e
    raise err

def build_input_key(template):
    data = ''
    for k in sorted(template):
        if template[k] is not None:
            data += str(template[k])
    return md5(data.encode()).hexdigest()

# individual VMs can be tracked viua a combination of labels and workflow meta
# Workflows are reported by the cromwell_driver output and then VMs can be tracked
# A combination of the Workflow label and the call- label should be able to uniquely identify tasks
# Scatter jerbs will have colliding VMs, but can that will have to be handled elsewhere
# Possibly, the task start pattern will contain multiple operation starts (or maybe shard-ids are given as one of the digit fields)

workflow_dispatch_pattern = re.compile(r'Workflows(( [a-z0-9\-]+,?)+) submitted.')
workflow_start_pattern = re.compile(r'WorkflowManagerActor Successfully started WorkflowActor-([a-z0-9\-]+)')
task_start_pattern = re.compile(r'\[UUID\((\w{8})\)(\w+)\.(\w+):(\w+):(\d+)\]: job id: ((?:projects/.+/)?operations/\S+)')
#(short code), (workflow name), (task name), (? 'NA'), (call id), (operation)
msg_pattern = re.compile(r'\[UUID\((\w{8})\)\]')
#(short code), message
fail_pattern = re.compile(r"ERROR - WorkflowManagerActor Workflow ([a-z0-9\-]+) failed \(during *?\): (.+)")
#(long id), (opt: during status), (failure message)
status_pattern = re.compile(r'PipelinesApiAsyncBackendJobExecutionActor \[UUID\(([a-z0-9\-]+)\)(\w+)\.(\w+):(\w+):(\d+)]: Status change from (.+) to (.+)')
#(short code), (workflow name), (task name), (? 'NA'), (call id), (old status), (new status)

instance_name_pattern = re.compile(r'instance(?:Name)?:\s+(.+)')

disk_pattern = re.compile(r'local-disk (\d+) (HDD|SSD)')

mtypes = {
    'n1-standard-%d'%(2**i): (0.0475*(2**i), 0.01*(2**i)) for i in range(7)
}
mtypes.update({
    'n1-highmem-%d'%(2**i): (.0592*(2**i), .0125*(2**i)) for i in range(1,7)
})
mtypes.update({
    'n1-highcpu-%d'%(2**i): (.03545*(2**i), .0075*(2**1)) for i in range(1,7)
})
mtypes.update({
    'f1-micro': (0.0076, 0.0035),
    'g1-small': (0.0257, 0.007)
})

core_price = (0.031611, 0.006655)
mem_price = (0.004237, 0.000892)
extended_price = (0.009550, 0.002014)

def get_hourly_cost(mtype, preempt=False):
    """
    Returns the cost/hour of a given GCP machine type.
    All machine types supported, including predefined n1, f1, and g1 instance families
    As well as custom and custom extended memory instances.
    If `preempt` is True, hourly cost will reflect preemptible price model
    """
    try:
        if mtype in mtypes:
            return mtypes[mtype][int(preempt)]
        else:
            if mtype.endswith('-ext'):
                mtype = mtype[:-4]
            custom, cores, mem = mtype.split('-')
            return (core_price[int(preempt)]*int(cores)) + (mem_price[int(preempt)]*max(int(mem), int(cores) * 1024 * 6.5)/1024) + max(0, extended_price[int(preempt)]*(int(mem)-(int(cores)*1024*6.5))/1024)
    except:
        traceback.print_exc()
        print(mtype, "unknown machine type")
        return 0

def get_cromwell_type(runtime):
    if runtime['memory'] <= 3:
        return 'n1-standard-1'
    return 'custom-2-%d' % (1024 * runtime['memory'])

class NoSuchSubmission(Exception):
    pass

class Recall(object):
    value = None
    def apply(self, value):
        self.value = value
        return value

def do_select(reader, t):
    if isinstance(reader, BytesIO):
        # print("Bytes seek")
        current = reader.tell()
        reader.seek(0,2)
        end = reader.tell()
        if current < end:
            # print("There are", end-current, "bytes")
            reader.seek(current, 0)
            return [[reader]]
        return [[]]
    else:
        return select.select([reader], [], [], t)


class Call(object):
    """
    Class represents a given Task in a workflow
    """
    status = '-'
    last_message = ''
    def __init__(self, parent, path, task, attempt, operation):
        self.parent = parent
        self.path = path
        self.task = task
        self.attempt = attempt
        self.operation = operation

    @property
    @cached(10)
    def return_code(self):
        """
        Property. The return code from the Task's script.
        10 second cache
        """
        try:
            blob = strict_getblob(os.path.join(self.path, 'rc'))
            return int(blob.download_as_string().decode())
        except FileNotFoundError:
            return None
        except ValueError:
            return None

    @property
    @cached(10)
    def runtime(self):
        """
        Property. The time in hours that the Task was running.
        10 second cache
        """
        try:
            data = get_operation_status(self.operation)
            delta = (
                parse_time(data['metadata']['endTime'])
                if 'endTime' in data['metadata']
                else datetime.datetime.utcnow()
            ) - parse_time(data['metadata']['startTime'])
            return delta.total_seconds() / 3600
        except:
            traceback.print_exc()
            return 0

    @cached(30)
    def read_log(self, log_type):
        if log_type not in {'stdout', 'stderr', 'google', 'cromwell'}:
            raise ValueError("log_type must be one of {'stdout', 'stderr', 'google', 'cromwell'}")
        filename, suffix = {
            'stdout': ('stdout', '-stdout.log'),
            'stderr': ('stderr', '-stderr.log'),
            'google': (None, '.log'),
            'cromwell': (None, '.log')
        }[log_type]
        idx = self.parent.calls.index(self)
        log_text = cache_fetch('workflow', self.parent.parent.submission, self.parent.long_id, dtype=str(idx)+'.', ext=log_type+'.log')
        if log_text is not None:
            return log_text
        blob = None
        if filename is not None:
            path = os.path.join(
                self.path,
                filename
            )
            print("Trying", path)
            blob = getblob(path)
            if not blob.exists():
                blob = None
        if blob is None:
            path = os.path.join(
                self.path,
                self.task + suffix
            )
            print("Trying", path)
            blob = strict_getblob(path)
        text = blob.download_as_string().decode()
        if not self.parent.parent.live:
            cache_write(text, 'workflow', self.parent.parent.submission, self.parent.long_id, dtype=str(idx)+'.', ext=log_type+'.log')
        return text

@cached(10)
def get_operation_status(opid, parse=True, fmt='json'):
    """
    Fetches the metadata for a given GCP operation ID.
    If `parse` is True (default) the json string will be parsed into a python dict.
    `fmt` sets the download format. Do not change unless you have reason to download
    in a different data format.
    10 second cache.
    """
    text = cache_fetch('operation', opid)
    if text is None:
        text = subprocess.run(
            'gcloud alpha genomics operations describe --format=%s %s' % (
                fmt,
                opid
            ),
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        ).stdout.decode()
        if not parse:
            return text
        try:
            data = yaml.safe_load(StringIO(text))
            if 'done' in data and data['done']:
                cache_write(text, 'operation', opid)
        except yaml.scanner.ScannerError:
            if 'Permission denied' in text:
                raise ValueError("Permission Denied")
            print(text)
            raise
    else:
        data = yaml.safe_load(StringIO(text))
    if not parse:
        return text
    return data

def abort_operation(opid):
    return subprocess.run(
        'yes | gcloud alpha genomics operations cancel %s' % (
            opid
        ),
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

def kill_machines(instance_name):
    # subprocess.check_call(
    #     'gcloud compute instances stop %s --async' % instance_name,
    #     shell=True
    # )
    # machines = subprocess.run(
    #     'gcloud compute instances list --filter="labels.%s"' % (
    #         label
    #     ),
    #     shell=True,
    #     executable='/bin/bash',
    #     stdout=subprocess.PIPE,
    #     stderr=subprocess.STDOUT,
    # ).stdout.decode().split('\n')
    # if len(machines) > 1:
    #     machines = ' '.join(line.split()[0] for line in machines[1:] if len(line.strip()))
    # if len(machines):
    # Use Popen instead of run because deleting instances is a slow, blocking operation
    return subprocess.Popen(
        'yes | gcloud compute instances delete %s' % (
            instance_name
        ),
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

class CommandReader(object):
    """
    Reads buffered output from a subprocess command.
    __init__ takes identical arguments to subprocess.Popen
    Call readline() to get one line of text
    """
    def __init__(self, cmd, *args, __insert_text=None, **kwargs):
        r,w = os.pipe()
        r2,w2 = os.pipe()
        if '__insert_text' in kwargs:
            __insert_text = kwargs['__insert_text']
            del kwargs['__insert_text']
        if __insert_text is not None:
            os.write(w, __insert_text)
        # self.proc = subprocess.Popen(cmd, *args, stdout=w, stderr=w, stdin=r2, universal_newlines=False, **kwargs)
        self.proc = subprocess.Popen(cmd, *args, stdout=w, stderr=subprocess.DEVNULL, stdin=r2, universal_newlines=False, **kwargs)
        self.reader = open(r, 'rb')
        self.buffer = b''

    def close(self, *args, **kwargs):
        """
        Closes the input stream and kills the underlying process
        """
        self.reader.close(*args, **kwargs)
        if self.proc.returncode is None:
            self.proc.kill()

    def readline(self, length=256, *args, **kwargs):
        """
        Reads a single line of text from the underlying process.
        `length` specifies the number of bytes that are read in each chunk while
        waiting for a complete line of text
        """
        while b'\n' not in self.buffer:
            self.buffer += os.read(self.reader.fileno(), length)
        self.buffer = self.buffer.split(b'\n')
        output = self.buffer.pop(0)
        self.buffer = b'\n'.join(self.buffer)
        return output

    def __getattr__(self, attr):
        if hasattr(self.reader, attr):
            return getattr(self.reader, attr)
        raise AttributeError("No such attribute '%s'" % attr)

    def __del__(self):
        self.close()

## TODO:
## 1) Controller:
##      Cache a cromwell reader and text from the most recent submission
##      On submission change, close the reader and reset text
##      On fetch: use select to check for input (50ms timeout) then read all
##      Optional line_offset input returns that line and all after
## 2) Adapter:

class SubmissionAdapter(object):
    """
    Represents a single Lapdog Submission.
    Use to interact with the submission, including fetching status and workflows
    or aborting a running submission
    """
    def __init__(self, bucket, submission, gateway=None):
        """
        Constructs the adapter. Requires the bucket id for the workspace and the
        submission id for the submission. To save time, you may also provide the
        Gateway object for this namespace, otherwise a new Gateway will be constructed.
        Downloads and parses the submission.json file for this submission.
        submission.json files are cached if the submission is done.
        """
        self.bucket = bucket
        self.submission = submission
        try:
            # print("Constructing adapter")
            self.path = os.path.join(
                'gs://'+bucket,
                'lapdog-executions',
                submission
            )
            gs_path = os.path.join(
                self.path,
                'submission.json'
            )
            self.data = cache_fetch('submission-json', bucket, submission)
            _do_cache_write = False
            if self.data is None:
                try:
                    self.data = strict_getblob(gs_path).download_as_string().decode()
                    self._cached = False
                    _do_cache_write = True
                except FileNotFoundError as e:
                    raise NoSuchSubmission from e
            else:
                self._cached = True
                self._remove_pointer()
            self.data = json.loads(self.data)
            if 'operation' not in self.data:
                print("<GATEWAY DEV> Delete submission", submission)
            self.workspace = self.data['workspace']
            self.namespace = self.data['namespace']
            self.identifier = self.data['identifier']
            self.operation = self.data['operation'] if 'operation' in self.data else 'NULL'
            self.raw_workflows = self.data['workflows']
            self.gateway = Gateway(self.namespace) if gateway is None else gateway
            self.workflow_mapping = {}
            self.thread = None
            self.workflows = {}
            self._internal_reader = None
            self.bucket = bucket
            self._update_mask = set()
            self.update_lock = threading.Lock()
            if _do_cache_write and not self.live:
                cache_write(json.dumps(self.data), 'submission-json', bucket, submission)
                self._cached = True
                self._remove_pointer()
        except:
            self._remove_pointer()
            raise

    def _init_workflow(self, short, key=None, long_id=None):
        if short not in self.workflows:
            self.workflows[short] = WorkflowAdapter(self, short, self.path, key, long_id)
        return self.workflows[short]

    def cost(self):
        """
        Estimates the cost of a submission.
        If the submission is not running, parse the workflow.json file to get the
        exact start/stop times for every call of every workflow.
        If the submission is running, workflow.json was not found, or any error was encountered during the above computation:
        Update the adapter, and estimate cost by parsing the operation metadata for
        each Call in the submission. This pathway stops if the computation takes more than 60 seconds.
        Cost is cached if the submission is not running and the first cost estimation pathway finishes.
        Returns a dictionary with the elapsed wall time, total cpu time, total cost,
        and overhead cost of the cromwell server
        """
        try:
            if not self.live:
                stored_cost = cache_fetch('submission', self.namespace, self.workspace, self.submission, dtype='cost')
                if stored_cost is not None:
                    return json.loads(stored_cost)
                try:
                    workflow_metadata = json.loads(strict_getblob(
                        os.path.join(
                            self.path,
                            'results',
                            'workflows.json'
                        )
                    ).download_as_string())
                    cost = 0
                    total = 0
                    status = self.status
                    if 'metadata' in status and 'startTime' in status['metadata']:
                        maxTime = (
                            parse_time(status['metadata']['endTime']) if 'endTime' in status['metadata']
                            else datetime.datetime.utcnow()
                        ) - parse_time(status['metadata']['startTime'])
                        maxTime = maxTime.total_seconds() / 3600
                    for wf in workflow_metadata:
                        if wf['workflow_metadata'] is not None and 'calls' in wf['workflow_metadata']:
                            for calls in wf['workflow_metadata']['calls'].values():
                                for call in calls:
                                    if 'end' in call:

                                        delta = (
                                            # Stupid horrible ugly timestampt
                                            parse_time(call['end']) - parse_time(call['start'])
                                        )
                                        delta = delta.total_seconds() / 3600
                                        # if delta > maxTime:
                                        #     maxTime = delta
                                        total += delta
                                        if 'jes' in call and 'machineType' in call['jes']:
                                            cost += delta * get_hourly_cost(
                                                call['jes']['machineType'].split('/')[-1],
                                                'preemptible' in call and call['preemptible']
                                            )
                                        if 'runtimeAttributes' in call and 'disks' in call['runtimeAttributes'] and disk_pattern.match(call['runtimeAttributes']['disks']):
                                            result = disk_pattern.match(call['runtimeAttributes']['disks'])
                                            if result.group(2) == 'HDD':
                                                # runtime * 1hr/1month * monthly price/GB * GB
                                                cost += delta * 0.0013698624132109968 * .04 * float(result.group(1))
                                            elif result.group(2) == 'SSD':
                                                cost += delta * 0.0013698624132109968 * .17 * float(result.group(1))
                                            if 'bootDiskSizeGb' in call['runtimeAttributes']:
                                                cost += delta * 0.0013698624132109968 * .04 * float(call['runtimeAttributes']['bootDiskSizeGb'])
                    cost += maxTime * get_hourly_cost(
                        get_cromwell_type(self.data['runtime']) if 'runtime' in self.data else 'n1-standard-1',
                        False
                    )
                    cost = int(cost * 100) / 100
                    result = {
                        'clock_h': maxTime,
                        'cpu_h': total,
                        'est_cost': cost,
                        'cromwell_overhead': int(maxTime * get_hourly_cost(
                            get_cromwell_type(self.data['runtime']) if 'runtime' in self.data else 'n1-standard-1',
                            False
                        ) * 100) / 100
                    }
                    cache_write(json.dumps(result), 'submission', self.namespace, self.workspace, self.submission, dtype='cost')
                    return result
                except:
                    # Try the slow route
                    print(traceback.format_exc(), file=sys.stderr)
                    print("Attempting slow cost calculation")
                    pass
            try:
                cost = 0
                maxTime = 0
                total = 0
                with ActiveTimeout(60) as timer:
                    self.update()
                    timer.update()
                    for wf in self.workflows.values():
                        for call in wf.calls:
                            timer.update()
                            try:
                                call = get_operation_status(call.operation)
                                delta = (
                                    parse_time(call['metadata']['endTime'])
                                    if 'endTime' in call['metadata']
                                    else datetime.datetime.utcnow()
                                ) - parse_time(call['metadata']['startTime'])
                                delta = delta.total_seconds() / 3600
                                # if delta > maxTime:
                                #     maxTime = delta
                                total += delta
                                if 'jes' in call and 'machineType' in call['jes']:
                                    cost += delta * get_hourly_cost(
                                        call['jes']['machineType'].split('/')[-1],
                                        'preemptible' in call['metadata']['request']['pipelineArgs']['resources']
                                        and call['metadata']['request']['pipelineArgs']['resources']['preemptible']
                                    )
                            except KeyError:
                                pass
                            except yaml.scanner.ScannerError:
                                print("Operation", call.operation, "had invalid operation metadata")
            except TimeoutExceeded:
                print("Took longer than 60 seconds to load workflow costs. Skipping")
            status = self.status
            if 'metadata' in status and 'startTime' in status['metadata']:
                maxTime = (
                    parse_time(status['metadata']['endTime']) if 'endTime' in status['metadata']
                    else datetime.datetime.utcnow()
                ) - parse_time(status['metadata']['startTime'])
                maxTime = maxTime.total_seconds() / 3600
            cost += maxTime * get_hourly_cost(
                get_cromwell_type(self.data['runtime']) if 'runtime' in self.data else 'n1-standard-1',
                False
            )
            cost = int(cost * 100) / 100
            return {
                'clock_h': maxTime,
                'cpu_h': total,
                'est_cost': cost,
                'cromwell_overhead': int(maxTime * get_hourly_cost(
                    get_cromwell_type(self.data['runtime']) if 'runtime' in self.data else 'n1-standard-1',
                    False
                ) * 100) / 100
            }
        except:
            traceback.print_exc()
            print("STRING FORMAT ERROR", self.submission)
            return {
                'clock_h': 0,
                'cpu_h': 0,
                'est_cost': 0,
                'cromwell_overhead': 0
            }

    def _remove_pointer(self):
        if cache_fetch('submission-pointer', self.bucket, self.submission) is not None:
            try:
                os.remove(cache_path('submission-pointer')(self.bucket, self.submission))
            except:
                print("Could not remove expired pointer entry")
                traceback.print_exc()

    @cached(90)
    def update(self, timeout=-1):
        """
        Fetch latest data from Cromwell Server to update the adapter.
        Specify a positive number for `timeout` to set the maximum time to wait
        to acquire the lock on the underlying data stream.
        If acquiring the lock takes more than 10 seconds, do not update the adapter.
        This is because another thread must have just finished an update operation,
        so there is no need to immediately re-update.
        90 second cache.
        """
        ctime = time.monotonic()
        try:
            with context_lock(self.update_lock, timeout):
                if time.monotonic() - ctime > 10:
                    # We've been waiting a while already
                    # no reason to re-update, if another thread just spent a while
                    return
                # if self._internal_reader is None:
                #     self._internal_reader = self.read_cromwell(_do_wait=self.live)
                event_stream = []
                message = ''
                reader = self.read_cromwell(_do_wait=self.live)
                while len(do_select(reader, 1)[0]):
                    message = reader.readline().decode().strip()
                    code = md5(message.encode()).digest()
                    matcher = Recall()
                    if matcher.apply(workflow_dispatch_pattern.search(message)):
                        if code in self._update_mask:
                            continue
                        self._update_mask.add(code)
                        # This event helps establish the order of workflows
                        # The message is posted with the exact order of workflow-ids
                        # Which will match the order of entities dispatched in the submission
                        # It creates a new workflow object and inserts the mapping
                        # of key->id into the table
                        ids = [
                            wf_id.strip() for wf_id in
                            matcher.value.group(1).split(',')
                        ]
                        # print(len(ids), 'workflow(s) dispatched')
                        for long_id, data in zip(ids, self.raw_workflows[len(self.workflow_mapping):]):
                            self._init_workflow(
                                long_id[:8],
                                data['workflowOutputKey'],
                                long_id
                            )
                            self.workflow_mapping[data['workflowOutputKey']] = long_id
                    if matcher.apply(workflow_start_pattern.search(message)):
                        if code in self._update_mask:
                            continue
                        self._update_mask.add(code)
                        # This event captures a start message for a workflow which somehow
                        # was not captured by the above dispatch event
                        long_id = matcher.value.group(1)
                        if long_id not in {*self.workflow_mapping.values()}:
                            warnings.warn("Unexpected state")
                            traceback.print_stack()
                            idx = len(self.workflow_mapping)
                            data = self.raw_workflows[idx]
                            self._init_workflow(
                                long_id[:8],
                                data['workflowOutputKey'],
                                long_id
                            )
                            self.workflow_mapping[data['workflowOutputKey']] = long_id

                    elif matcher.apply(task_start_pattern.search(message)):
                        if code in self._update_mask:
                            continue
                        self._update_mask.add(code)
                        short = matcher.value.group(1)
                        wf = matcher.value.group(2)
                        task = matcher.value.group(3)
                        na = matcher.value.group(4)
                        call = int(matcher.value.group(5))
                        operation = matcher.value.group(6)
                        self._init_workflow(short).handle(
                            'task',
                            wf,
                            task,
                            na,
                            call,
                            operation
                        )
                    elif matcher.apply(msg_pattern.search(message)):
                        if code in self._update_mask:
                            continue
                        self._update_mask.add(code)
                        self._init_workflow(matcher.value.group(1)).handle(
                            'message',
                            matcher.value.string
                        )
                    elif matcher.apply(fail_pattern.search(message)):
                        if code in self._update_mask:
                            continue
                        self._update_mask.add(code)
                        long_id = matcher.value.group(1)
                        self._init_workflow(long_id[:8]).handle(
                            'fail',
                            matcher.groups()[-1]
                        )
                    elif matcher.apply(status_pattern.search(message)):
                        if code in self._update_mask:
                            continue
                        self._update_mask.add(code)
                        short = matcher.value.group(1)
                        task = matcher.value.group(3)
                        call = int(matcher.value.group(5))
                        old = matcher.value.group(6)
                        new = matcher.value.group(7)
                        self._init_workflow(short).handle(
                            'status',
                            task,
                            call,
                            old,
                            new
                        )
        except TimeoutExceeded:
            pass
            # else:
            #     print("NO MATCH:", message)

    def update_data(self):
        """
        Fetches latest submission data
        """
        if self._cached:
            return
        with self.update_lock:
            gs_path = os.path.join(
                self.path,
                'submission.json'
            )
            self.data = json.loads(strict_getblob(gs_path).download_as_string().decode())
            if 'operation' not in self.data:
                print("<GATEWAY DEV> Delete submission", submission)
            self.workspace = self.data['workspace']
            self.namespace = self.data['namespace']
            self.identifier = self.data['identifier']
            self.operation = self.data['operation'] if 'operation' in self.data else 'NULL'

    @property
    def input_mapping(self):
        return {
            build_input_key(row.to_dict()):row.to_dict()
            for i, row in self.config.iterrows()
        }

    @property
    @cached(60)
    def config(self):
        config = cache_fetch('submission-config', self.bucket, self.submission)
        if config is None:
            config = strict_getblob(self.path+'/config.tsv').download_as_string().decode()
            cache_write(config, 'submission-config', self.bucket, self.submission)
        return pd.read_csv(StringIO(config), sep='\t')


    def abort(self):
        """
        Abort a running workflow.
        If the current status is "Running", send a soft abort, giving the Cromwell
        server time to cancel running workflows and clean up. Set status to "Aborting".

        If the abort procedure is taking too long, you may abort again:
        If the current status is "Aborting", send a hard abort, immediately killing
        the Cromwell server. This may leave running workflows in an orphan state
        where they continue to run until completion with no way to halt them.
        Forcefully set status to "Aborted".
        """
        # self.update()
        # FIXME: Once everything else works, see if cromwell labels work
        # At that point, we can add an abort here to kill everything with the id
        status = self.status
        if 'done' in status and status['done']:
            return
        try:
            # For now, use abort key
            result = self.gateway.abort_submission(
                self.bucket,
                self.data['submission_id'],
                self.data['status'] == 'Aborting' # Hard abort if we're already aborting
            )
            if result is not None:
                # Failed
                warnings.warn(
                    "Submission Abort did not complete. Some workflows may continue running unsupervised",
                    RuntimeWarning
                )
                print(result.text, file=sys.stderr)
        except:
            traceback.print_exc()
            warnings.warn(
                "Submission Abort did not complete. Some workflows may continue running unsupervised",
                RuntimeWarning
            )
        finally:
            gs_path = os.path.join(
                self.path,
                'submission.json'
            )
            getblob(gs_path).upload_from_string(
                json.dumps(
                    {
                        **self.data,
                        **{'status': 'Aborted' if self.data['status'] == 'Aborting' else 'Aborting'}
                    }
                )
            )




    @property
    @cached(5)
    def status(self):
        """
        Property. Get the operation status
        5 second cache
        """
        # print("READING ADAPTER STATUS")
        return get_operation_status(self.operation)

    @property
    @cached(10)
    def live(self):
        """
        Property. Reports if the submission is active or not
        10 second cache
        """
        status = self.status
        return not ('done' in status and status['done'])

    def read_cromwell(self, _do_wait=True):
        """
        Attempts to open a data stream to the cromwell server.
        Currently, the data stream is a BytesIO object of the most recent log
        output.
        In the future, submissions will use an Stackdriver event stream
        and fallback to file logs.
        If the submission has been recently started, this blocks for ~2 minutes
        """
        status = self.status # maybe this shouldn't be a property...it takes a while to load
        cromwell_text = cache_fetch('submission', self.namespace, self.workspace, self.submission, dtype='cromwell', decode=False)
        if cromwell_text is not None:
            return BytesIO(cromwell_text)
        while 'metadata' not in status or ('startTime' not in status['metadata'] and 'endTime' not in status['metadata']):
            status = self.status
            time.sleep(1)
        if _do_wait:
            sleep_until(
                parse_time(status['metadata']['startTime'])
                + utc_offset
                + datetime.timedelta(seconds=120)
            )
        stdout_blob = getblob(os.path.join(
            'gs://'+self.bucket,
            'lapdog-executions',
            self.submission,
            'logs',
            'stdout.log'
        ))
        if stdout_blob.exists():
            log_text = stdout_blob.download_as_string()
            cache_write(log_text, 'submission', self.namespace, self.workspace, self.submission, dtype='cromwell', decode=False)
            return BytesIO(log_text)
        stdout_blob = getblob(os.path.join(
            'gs://'+self.bucket,
            'lapdog-executions',
            self.submission,
            'logs',
            'pipeline-stdout.log'
        ))
        if stdout_blob.exists():
            log_text = stdout_blob.download_as_string()
            return BytesIO(log_text)
        stdout_blob = getblob(os.path.join(
            'gs://'+self.bucket,
            'lapdog-executions',
            self.submission,
            'logs',
            self.operation[11:]+'-stdout.log'
        ))
        if stdout_blob.exists():
            log_text = stdout_blob.download_as_string()
            cache_write(log_text, 'submission', self.namespace, self.workspace, self.submission, dtype='cromwell', decode=False)
            return BytesIO(log_text)
        if self.data['status'] != 'Error' and 'done' in status and status['done']:
            # If we get here, the submission is done, but there were no logs
            self.data['status'] = 'Error'
            cache_write(json.dumps(self.data), 'submission-json', self.bucket, self.submission)
            gs_path = os.path.join(
                self.path,
                'submission.json'
            )
            getblob(gs_path).upload_from_string(json.dumps(self.data).encode())
        return BytesIO(b'')


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


"""
Identifying workflows:
Workflows tend to be identified by 3 different IDs within lapdog
1) Output Key (WorkflowAdapter.key): This ID is generated based on the inputs to
the workflow. It is used internally by lapdog because it can be assigned early on
and is deterministic. Output keys are used to align task outputs with the proper
entities when uploading results
2) Long ID (WorkflowAdapter.long_id) : This ID is assigned by the submission's cromwell server.
It uniquely identifies the workflow, but is not assigned until the workflow is dispatched.
Long IDs are used to match certain cromwell events to the right Workflow
3) Short ID (WorkflowAdapter.id) : This ID is the 8 character prefix of the Long ID.
Short IDs are often more convenient to display, and are used to match status messages
to the right Workflow
"""

class WorkflowAdapter(object):
    """
    Represents a single workflow.
    It is best not to manipulate this object directly as it is very closely connected
    to its parent SubmissionAdapter.

    Do not call the `handle` method or any of the `on_...` methods as this may result
    in your WorkflowAdapter being stuck in an invalid state
    """
    # this adapter needs to be initialized from an input key and a cromwell workflow id (short or long)
    # At first, dispatching events fills the replay buffer
    # when the workflow is started, the buffer is played and the workflow updates to current state DAWG

    def __init__(self, parent, short_id, parent_path, input_key=None, long_id=None):
        self.parent = parent
        self.id = short_id
        self.key = input_key
        self.long_id = long_id
        self.replay_buffer = []
        self.started = long_id is not None
        self.calls = []
        self.last_message = ''
        self.path = None
        self.parent_path = parent_path
        self.failure = None

    @property
    def inputs(self):
        return self.parent.input_mapping[
            {v:k for k,v in self.parent.workflow_mapping.items()}[self.long_id]
        ]


    @property
    def status(self):
        """
        Property. Get the status of the most recent Call to start
        """
        if len(self.calls):
            status = self.calls[-1].status
            if status == '-':
                return 'Starting'
            return status
        return 'Pending'

    def handle(self, evt, *args, **kwargs):
        if not self.started:
            self.replay_buffer.append((evt, args, kwargs))
            return
        attribute = 'on_'+evt
        getattr(self, attribute)(*args, **kwargs)

    def on_start(self, input_key, long_id):
        # print("Handling start event")
        self.started = True
        self.long_id = long_id
        self.input_key = input_key
        for event, args, kwargs in self.replay_buffer:
            # print("Replaying previous events...")
            self.handle(event, *args, **kwargs)
        self.replay_buffer = []

    def on_task(self, workflow, task, na, call, operation):
        # print("Starting task", workflow, task, na, call, operation)
        path = os.path.join(
            self.parent_path,
            'workspace',
            workflow,
            self.long_id,
            'call-'+task
        )
        if call > 1:
            path = os.path.join(path, 'attempt-'+str(call))
        self.calls.append(Call(
            self,
            path,
            task,
            call,
            operation
        ))

    def on_message(self, message):
        pass
        # if len(self.calls):
        #     self.calls[-1].last_message = message.strip()
        # else:
            # print("Discard message", message)

    def on_fail(self, message, status=None):
        self.failure = message

    def on_status(self, task, attempt, old, new):
        for call in self.calls:
            if call.task == task and call.attempt == attempt:
                call.status = new
                return
        # else:
            # print("Discard status", old,'->', new)

    # def abort(self):
    #     for call in self.calls:
    #         print("Aborting", call.operation)
    #         abort_operation(call)
    #         status = get_operation_status(call.operation, False)
    #         result = instance_name_pattern.search(status)
    #         if result:
    #             kill_machines(result.group(1).strip())
        # if len(self.calls):
        #     # print("Aborting", self.calls[-1].operation)
        #     abort_operation(self.calls[-1].operation)
        # if self.long_id is not None:
        #     kill_machines('cromwell-workflow-id=cromwell-'+self.long_id)



# wf = WFAdapter(input_key, short_id, long_id=None)
# wf.handle(event)
# ...
# wf.handle(start_event)
# |
# |_ [self.handle(event) for event in self.replay_buffer]
