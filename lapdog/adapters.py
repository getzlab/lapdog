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
import traceback
import sys

# Label filter format: labels.(label name)=(label value)

timestamp_format = '%Y-%m-%dT%H:%M:%SZ'
utc_offset = datetime.datetime.fromtimestamp(time.time()) - datetime.datetime.utcfromtimestamp(time.time())

def sleep_until(dt):
    sleep_time = (dt - datetime.datetime.now()).total_seconds()
    if sleep_time > 0:
        time.sleep(sleep_time)

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
    try:
        if mtype in mtypes:
            return mtypes[mtype][int(preempt)]
        else:
            custom, cores, mem = mtype.split('-')
            return (core_price[int(preempt)]*int(cores)) + (mem_price[int(preempt)]*int(mem)/1024) + max(0, extended_price[int(preempt)]*(int(mem)-13312)/1024)
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

def safe_getblob(gs_path):
    blob = getblob(gs_path)
    if not blob.exists():
        raise FileNotFoundError("No such blob: "+gs_path)
    return blob

def getblob(gs_path, user_project=None):
    bucket_id = gs_path[5:].split('/')[0]
    bucket_path = '/'.join(gs_path[5:].split('/')[1:])
    return storage.Blob(
        bucket_path,
        storage.Client().bucket(bucket_id, user_project)
    )

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
    status = '-'
    last_message = ''
    def __init__(self, path, task, attempt, operation):
        self.path = path
        self.task = task
        self.attempt = attempt
        self.operation = operation

    @property
    @cached(10)
    def return_code(self):
        try:
            blob = safe_getblob(os.path.join(self.path, 'rc'))
            return int(blob.download_as_string().decode())
        except FileNotFoundError:
            return None
        except ValueError:
            return None

@cached(10)
def get_operation_status(opid, parse=True, fmt='json'):
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
            data = yaml.load(StringIO(text))
            if 'done' in data and data['done']:
                cache_write(text, 'operation', opid)
        except yaml.scanner.ScannerError:
            if 'Permission denied' in text:
                raise ValueError("Permission Denied")
            print(text)
            raise
    else:
        data = yaml.load(StringIO(text))
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
    def __init__(self, cmd, *args, __insert_text=None, **kwargs):
        r,w = os.pipe()
        r2,w2 = os.pipe()
        if '__insert_text' in kwargs:
            __insert_text = kwargs['__insert_text']
            del kwargs['__insert_text']
        if __insert_text is not None:
            os.write(w, __insert_text)
        # self.proc = subprocess.Popen(cmd, *args, stdout=w, stderr=w, stdin=r2, universal_newlines=False, **kwargs)
        self.proc = subprocess.Popen(cmd, *args, stdout=w, stderr=w, stdin=r2, universal_newlines=False, **kwargs)
        self.reader = open(r, 'rb')
        self.buffer = b''

    def close(self, *args, **kwargs):
        self.reader.close(*args, **kwargs)
        if self.proc.returncode is None:
            self.proc.kill()

    def readline(self, length=256, *args, **kwargs):
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
    def __init__(self, bucket, submission):
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
                self.data = safe_getblob(gs_path).download_as_string().decode()
                _do_cache_write = True
            except FileNotFoundError as e:
                raise NoSuchSubmission from e
        self.data = json.loads(self.data)
        self.workspace = self.data['workspace']
        self.namespace = self.data['namespace']
        self.identifier = self.data['identifier']
        self.operation = self.data['operation']
        self.raw_workflows = self.data['workflows']
        self.workflow_mapping = {}
        self.thread = None
        self.bucket = bucket
        self.submission = submission
        self.workflows = {}
        self._internal_reader = None
        self.bucket = bucket
        if _do_cache_write and not self.live:
            cache_write(json.dumps(self.data), 'submission-json', bucket, submission)

    def _init_workflow(self, short, key=None, long_id=None):
        if short not in self.workflows:
            self.workflows[short] = WorkflowAdapter(short, self.path, key, long_id)
        return self.workflows[short]

    def cost(self):
        try:
            if not self.live:
                stored_cost = cache_fetch('submission', self.namespace, self.workspace, self.submission, dtype='cost')
                if stored_cost is not None:
                    return json.loads(stored_cost)
                try:
                    workflow_metadata = json.loads(safe_getblob(
                        os.path.join(
                            self.path,
                            'results',
                            'workflows.json'
                        )
                    ).download_as_string())
                    cost = 0
                    maxTime = 0
                    total = 0
                    for wf in workflow_metadata:
                        if wf['workflow_metadata'] is not None:
                            for calls in wf['workflow_metadata']['calls'].values():
                                for call in calls:
                                    if 'end' in call:
                                        delta = datetime.datetime.strptime(call['end'].split('.')[0], '%Y-%m-%dT%H:%M:%S') - datetime.datetime.strptime(call['start'].split('.')[0], '%Y-%m-%dT%H:%M:%S')
                                        delta = delta.total_seconds() / 3600
                                        if delta > maxTime:
                                            maxTime = delta
                                        total += delta
                                        if 'jes' in call and 'machineType' in call['jes']:
                                            cost += delta * get_hourly_cost(
                                                call['jes']['machineType'].split('/')[-1],
                                                'preemptible' in call and call['preemptible']
                                            )
                    cost += maxTime * get_hourly_cost(
                        get_cromwell_type(self.data['runtime']) if 'runtime' in self.data else 'n1-standard-1',
                        False
                    )
                    cost = int(cost * 100) / 100
                    result = {
                        'clock_h': maxTime,
                        'cpu_h': total,
                        'est_cost': cost
                    }
                    cache_write(json.dumps(result), 'submission', self.namespace, self.workspace, self.submission, dtype='cost')
                    return result
                except:
                    # Try the slow route
                    print(traceback.format_exc(), file=sys.stderr)
                    print("Attempting slow cost calculation")
                    pass
            self.update()
            cost = 0
            maxTime = 0
            total = 0
            for wf in self.workflows.values():
                for call in wf.calls:
                    try:
                        call = get_operation_status(call.operation)
                        delta = (
                            datetime.datetime.strptime(
                                (call['metadata']['endTime'].split('.')[0]+'Z').replace('ZZ', 'Z'), # stupid and ugly
                                timestamp_format
                            )
                            if 'endTime' in call['metadata']
                            else datetime.datetime.utcnow()
                        ) - datetime.datetime.strptime(
                            (call['metadata']['startTime'].split('.')[0]+'Z').replace('ZZ', 'Z'), # stupid and ugly
                            timestamp_format
                        )
                        delta = delta.total_seconds() / 3600
                        if delta > maxTime:
                            maxTime = delta
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
            status = self.status
            if 'metadata' in status and 'startTime' in status['metadata']:
                delta = (
                    datetime.datetime.strptime(
                        status['metadata']['endTime'],
                        timestamp_format
                    ) if 'endTime' in status['metadata']
                    else datetime.datetime.utcnow()
                ) - datetime.datetime.strptime(
                    status['metadata']['startTime'],
                    timestamp_format
                )
                delta = delta.total_seconds() / 3600
                if delta > maxTime:
                    maxTime = delta
            cost += maxTime * get_hourly_cost(
                get_cromwell_type(self.data['runtime']) if 'runtime' in self.data else 'n1-standard-1',
                False
            )
            cost = int(cost * 100) / 100
            return {
                'clock_h': maxTime,
                'cpu_h': total,
                'est_cost': cost
            }
        except:
            traceback.print_exc()
            print("STRING FORMAT ERROR", self.submission)
            return {
                'clock_h': 0,
                'cpu_h': 0,
                'est_cost': 0
            }


    def update(self):
        if self._internal_reader is None:
            self._internal_reader = self.read_cromwell(_do_wait=self.live)
        event_stream = []
        message = ''
        while len(do_select(self._internal_reader, 1)[0]):
            message = self._internal_reader.readline().decode().strip()
            matcher = Recall()
            if matcher.apply(workflow_dispatch_pattern.search(message)):
                ids = [
                    wf_id.strip() for wf_id in
                    matcher.value.group(1).split(',')
                ]
                # print(len(ids), 'workflow(s) dispatched')
                for long_id, data in zip(ids, self.raw_workflows):
                    self._init_workflow(
                        long_id[:8],
                        data['workflowOutputKey'],
                        long_id
                    )
                    self.workflow_mapping[data['workflowOutputKey']] = long_id
            elif matcher.apply(workflow_start_pattern.search(message)):
                # print("New workflow started")
                long_id = matcher.value.group(1)
                if long_id in {*self.workflow_mapping.values()}:
                    # dispatch event on fully started workflow
                    self.workflows[long_id[:8]].handle(
                        'start',
                        {v:k for k,v in self.workflow_mapping.items()}[long_id],
                        long_id
                    )
                elif long_id[:8] in self.workflows:
                    # dispatch event on half-started workflow
                    # this will syncup and replay events
                    idx = len(self.workflow_mapping)
                    data = self.raw_workflows[idx]
                    self.workflows[long_id[:8]].handle(
                        'start',
                        data['workflowOutputKey'],
                        long_id
                    )
                    self.workflow_mapping[data['workflowOutputKey']] = long_id
                else:
                    # instead initialize new workflow
                    idx = len(self.workflow_mapping)
                    data = self.raw_workflows[idx]
                    self._init_workflow(
                        long_id[:8],
                        data['workflowOutputKey'],
                        long_id
                    )
                    self.workflow_mapping[data['workflowOutputKey']] = long_id
            elif matcher.apply(task_start_pattern.search(message)):
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
                self._init_workflow(matcher.value.group(1)).handle(
                    'message',
                    matcher.value.string
                )
            elif matcher.apply(fail_pattern.search(message)):
                long_id = matcher.value.group(1)
                self._init_workflow(long_id[:8]).handle(
                    'fail',
                    matcher.groups()[-1]
                )
            elif matcher.apply(status_pattern.search(message)):
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
            # else:
            #     print("NO MATCH:", message)

    def abort(self):
        self.update()
        # FIXME: Once everything else works, see if cromwell labels work
        # At that point, we can add an abort here to kill everything with the id
        abort_operation(self.operation)
        try:
            for wf in self.workflows.values():
                wf.abort()
            # kill_machines('lapdog-submission-id='+self.submission)
            status = get_operation_status(self.operation, False)
            result = instance_name_pattern.search(status)
            if result:
                kill_machines(result.group(1).strip())
        finally:
            gs_path = os.path.join(
                self.path,
                'submission.json'
            )
            getblob(gs_path).upload_from_string(
                json.dumps(
                    {
                        **self.data,
                        **{'status': 'Aborted'}
                    }
                )
            )



    @property
    @cached(2)
    def status(self):
        """
        Get the operation status
        """
        # print("READING ADAPTER STATUS")
        return get_operation_status(self.operation)

    @property
    def live(self):
        """
        Reports if the submission is active or not
        """
        status = self.status
        return not ('done' in status and status['done'])

    def read_cromwell(self, _do_wait=True):
        """
        Returns a file-object which reads stdout from the submission Cromwell VM
        """
        status = self.status # maybe this shouldn't be a property...it takes a while to load
        cromwell_text = cache_fetch('submission', self.namespace, self.workspace, self.submission, dtype='cromwell', decode=False)
        if cromwell_text is not None:
            return BytesIO(cromwell_text)
        while 'metadata' not in status or 'startTime' not in status['metadata']:
            status = self.status
            time.sleep(1)
        if _do_wait:
            sleep_until(
                datetime.datetime.strptime(
                    status['metadata']['startTime'],
                    timestamp_format
                )
                + utc_offset
                + datetime.timedelta(seconds=45)
            )
        if not ('done' in status and status['done']):
            cmd = (
                "gcloud compute ssh --zone {zone} {instance_name} -- "
                "'docker logs -f $(docker ps -q)'"
            ).format(
                zone=status['metadata']['runtimeMetadata']['computeEngine']['zone'],
                instance_name=status['metadata']['runtimeMetadata']['computeEngine']['instanceName']
            )
            return CommandReader(cmd, shell=True, executable='/bin/bash')
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
        # If we get here, the submission is done, but there were no logs
        if self.data['status'] != 'Error':
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


class WorkflowAdapter(object):
    # this adapter needs to be initialized from an input key and a cromwell workflow id (short or long)
    # At first, dispatching events fills the replay buffer
    # when the workflow is started, the buffer is played and the workflow updates to current state DAWG

    def __init__(self, short_id, parent_path, input_key=None, long_id=None):
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
    def status(self):
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

    def abort(self):
        for call in self.calls:
            print("Aborting", call.operation)
            abort_operation(call)
            status = get_operation_status(call.operation, False)
            result = instance_name_pattern.search(status)
            if result:
                kill_machines(result.group(1).strip())
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
