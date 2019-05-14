import os
import json
import dalmatian as dog
from firecloud import api as fc
from dalmatian import getblob, copyblob, moveblob, strict_getblob
from dalmatian.wmanager import _synchronized, _read_from_cache
import contextlib
import csv
from google.cloud import storage
from agutil.parallel import parallelize, parallelize2
from agutil import status_bar, byteSize, cmd as execute_command
from threading import Lock, Thread
import sys
import re
import tempfile
import time
import subprocess
from hashlib import md5
import base64
import yaml
from glob import glob, iglob
from io import StringIO
from . import adapters
from .adapters import get_operation_status, mtypes, NoSuchSubmission, CommandReader, build_input_key
from .cache import cache_init, cache_path
from .cloud.utils import proxy_group_for_user
from .gateway import Gateway, creation_success_pattern
from itertools import repeat
import pandas as pd
from socket import gethostname
from math import ceil
from functools import wraps, lru_cache
import traceback
import warnings
import shutil
import numpy as np
import requests
import pickle

lapdog_id_pattern = re.compile(r'[0-9a-f]{32}')
global_id_pattern = re.compile(r'lapdog/(.+)')
lapdog_submission_pattern = re.compile(r'.*?/?lapdog-executions/([0-9a-f]{32})/submission.json')
lapdog_submission_member_pattern = re.compile(r'.*?/?lapdog-executions/([0-9a-f]{32})/')

timestamp_format = '%Y-%m-%dT%H:%M:%S.000%Z'

class ConfigNotFound(KeyError):
    pass

class ConfigNotUnique(KeyError):
    pass

@contextlib.contextmanager
def dalmatian_api():
    """
    Context manager to convert AssertionErrors from dalmatian code into APIExceptions
    """
    try:
        yield
    except AssertionError as e:
        raise dog.APIException("The Firecloud API has returned an unknown failure condition") from e

@contextlib.contextmanager
def open_if_string(obj, mode, *args, **kwargs):
    if isinstance(obj, str):
        with open(obj, mode, *args, **kwargs) as f:
            yield f
    else:
        yield obj

@contextlib.contextmanager
def dump_if_file(obj):
    if isinstance(obj, str):
        yield obj
    else:
        data = obj.read()
        mode = 'w' + ('b' if isinstance(data, bytes) else '')
        with tempfile.NamedTemporaryFile(mode) as tmp:
            tmp.write(data)
            tmp.flush()
            yield tmp.name

def check_api(result):
    if result.status_code >= 400:
        raise dog.APIException("The Firecloud API has returned status %d : %s" % (result.status_code, result.text))
    return result

@lru_cache(1)
def _gsutil_available():
    return subprocess.run('which gsutil', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0

def list_potential_submissions(bucket_id):
    """
    Lists submission.json files found in a given bucket
    """
    if _gsutil_available():
        # Reading the gsutil output is slightly faster
        for line in re.finditer(r'.+submission\.json', subprocess.run(
                    'gsutil ls gs://{}/lapdog-executions/*/submission.json'.format(bucket_id),
                    shell=True,
                    stdout=subprocess.PIPE
                    ).stdout.decode()):
            yield line.group(0)
    else:
        # Using google-cloud-storage will always work, but is crazy slow
        warnings.warn("gsutil is not available. Using slower google-cloud-storage backend")
        for page in storage.Client().bucket(bucket_id).list_blobs(prefix="lapdog-executions").pages:
            for blob in page:
                if blob.exists() and lapdog_submission_pattern.match(blob.name):
                    yield 'gs://{}/{}'.format(bucket_id, blob.name)


@parallelize2(5)
def upload(bucket, path, source, allow_composite=True):
    """
    Uploads {source} to google cloud.
    Result google cloud path is gs://{bucket}/{path}.
    If the file to upload is larger than 4Gib, the file will be uploaded via
    a composite upload.

    WARNING: You MUST set allow_composite to False if you are uploading to a nearline
    or coldline bucket. The composite upload will incur large fees from deleting
    the temporary objects

    This function starts the upload on a background thread and returns a callable
    object which can be used to wait for the upload to complete. Calling the object
    blocks until the upload finishes, and will raise any exceptions encountered
    by the background thread. This function allows up to 5 concurrent uploads, beyond
    which workers will be queued until there is an empty execution slot.
    """
    # 4294967296
    blob = bucket.blob(path)
    if allow_composite and os.path.isfile(source) and os.path.getsize(source) >= 2865470566: #2.7gib
        blob.chunk_size = 104857600 # ~100mb
    blob.upload_from_filename(source)
    return blob


def purge_cache():
    """
    Empties the Lapdog Offline Disk Cache.
    This will force all cached data to be reloaded.
    Use if the disk cache has become too large.
    Lapdog will run slowly after calling this function until the cache is rebuilt
    """
    shutil.rmtree(cache_init())

def prune_cache():
    """
    Cleans the Lapdog Offline Disk Cache.
    This will remove cached files which have not been used in at least 30 days.
    The removed cache data will need to be reloaded next time it is requested by Lapdog.
    Use if the disk cache has become too large.
    This should not have a large impact on Lapdog runtime, as the pruned files are not commonly used.
    Returns the size of data cleaned
    """
    deleted = 0
    kept = 0
    t0 = time.time()
    for filepath in iglob(os.path.join(cache_init(), '*', '*', '*')):
        size = os.path.getsize(filepath)
        if (t0 - os.stat(filepath).st_atime) > 2628001:
            try:
                os.remove(filepath)
                deleted += size
            except:
                pass
        else:
            kept += size
    print("Removed", byteSize(deleted), "of unused cache entries")
    print("Kept", byteSize(kept), "of active cache entries")
    return deleted


def alias(func):
    """
    Use to define an alias for an existing function.
    Replaces the decorated function with the provided object
    """
    def wrapper(alias_func):
        return func
    return wrapper

def complete_execution(submission_id):
    """
    Checks a GCP job status and returns results to firecloud, if possible
    """
    if submission_id.startswith('lapdog/'):
        ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
        return WorkspaceManager(ns, ws).complete_execution(sid)
    raise TypeError("Global complete_execution can only operate on lapdog global ids")

def get_submission(submission_id):
    """
    Gets submission metadata from a lapdog or firecloud submission
    """
    if submission_id.startswith('lapdog/'):
        ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
        return WorkspaceManager(ns, ws).get_submission(sid)
    raise TypeError("Global get_submission can only operate on lapdog global ids")

def get_adapter(submission_id):
    """
    Gets adapter for a lapdog submission
    """
    if submission_id.startswith('lapdog/'):
        ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
        return WorkspaceManager(ns, ws).get_adapter(sid)
    raise TypeError("Global get_adapter can only operate on lapdog global ids")

def firecloud_status():
    obj = {
        'health': requests.get('https://api.firecloud.org/health').text,
        'systems': {}
    }
    for key, val in requests.get('https://api.firecloud.org/status').json()['systems'].items():
        obj['systems'][key] = 'ok' in val and val['ok']
    for key, val in requests.get('https://rawls.dsde-prod.broadinstitute.org/status').json()['systems'].items():
        obj['systems'][key] = 'ok' in val and val['ok']
    return obj

@parallelize(5)
def _load_submissions(wm, path):
    if '-json' in path:
        with open(path, 'r') as r:
            return path[-37:-5], json.load(r)
    elif '-ptr' in path:
        try:
            with open(path, 'r') as r:
                ns,ws,sid = r.read().split('/')
            if ns == wm.namespace and ws == wm.workspace:
                return sid, wm.get_adapter(sid).data
        except:
            traceback.print_exc()
    return path, None

@lru_cache()
def get_current_user():
    from .gateway import get_access_token, get_token_info
    return get_token_info(get_access_token())['email']

class WorkspaceManager(dog.WorkspaceManager):
    """
    Core Lapdog Class. Represents a single FireCloud workspace.
    Inherits from dalmatian.WorkspaceManager.
    Any features from dalmatian which are not explicitly upgraded by lapdog will
    still be accessible via inheritance
    """
    def __init__(self, reference, workspace=None, timezone='America/New_York', *, workspace_seed_url="http://localhost:4201"):
        """
        Various argument configurations:
        * Dalmatian-style: Workspace('namespace', 'workspace')
        * Lapdog-style: Workspace('namespace/workspace')

        workspace_seed_url controls the url that will be queried to attempt to pre-populate the operator cache with data from
        a running lapdog UI. set to None to disable this feature
        Returns a Workspace object
        """
        super().__init__(
            reference,
            workspace,
            timezone
        )
        self._webcache_ = False
        if workspace_seed_url is not None:
            try:
                self.cache = {
                    key:pickle.loads(base64.b64decode(value.encode()))
                    for key, value in requests.get(
                        workspace_seed_url+"/api/v1/workspaces/{namespace}/{workspace}/cache/seed".format(
                            namespace=self.namespace,
                            workspace=self.workspace
                        )
                    ).json().items()
                }
            except requests.ConnectionError:
                pass # UI probably not running; ignore
            except:
                traceback.print_exc()
                warnings.warn("Failed to pre-seed workspace cache from running Lapdog UI")
        self.gateway = Gateway(self.namespace)
        self._submission_cache = {}
        try:
            bucket_id = self.bucket_id
            target_prefix = 'submission-json.{}'.format(bucket_id)
            pointer_prefix = 'submission-ptr.{}'.format(bucket_id)
            self._submission_cache = {
                k:v
                for k,v in _load_submissions(
                    repeat(self),
                    (os.path.join(path, f)
                    for path, _, files in os.walk(cache_init())
                    for f in files
                    if f.startswith(target_prefix) or f.startswith(pointer_prefix))
                )
                if v is not None
            }

        except:
            print("Warning: Unable to prepopulate workspace submission cache. Workspace may not exist", file=sys.stderr)
            self.sync()

    def initialize_hound(self, credentials=None, user_project=None):
        """
        Initializes the HoundClient for the workspace, if it is None
        credentials: (optional) google.cloud.auth.Credentials to use when
        interacting with bucket, if not using default credentials
        user_project: (optional) name of project to bill, if bucket is requester pays
        """
        hound = super().initialize_hound(credentials, user_project)
        hound.author = get_current_user()
        return hound

    def populate_cache(self):
        """
        Preloads all data from the FireCloud workspace into the in-memory cache.
        Use in advance of switching offline so that the WorkspaceManager can run in
        offline mode without issue.

        Call `WorkspaceManager.go_offline()` after this function to switch
        the workspace into offline mode
        """
        super().populate_cache()
        for config in self.configs:
            try:
                self.get_wdl(
                    config['methodRepoMethod']['methodNamespace'],
                    config['methodRepoMethod']['methodName'],
                    config['methodRepoMethod']['methodVersion']
                )
            except NameError:
                # WDL Doesnt exist
                pass
        self.sync()

    def create_workspace(self, parent=None):
        """
        Creates the workspace.
        You may provide a WorkspaceManager as the parent to clone from
        """
        result = super().create_workspace(parent)
        if result:
            def update_acl():
                time.sleep(30)
                try:
                    from .gateway import get_access_token, get_token_info
                    with self.initialize_hound().with_reason('<Automated> Auto-add lapdog proxy-group to workspace'):
                        response = self.update_acl({
                            proxy_group_for_user(get_token_info(get_access_token())['email'])+'@firecloud.org': 'WRITER'
                        })
                except:
                    traceback.print_exc()
                    warnings.warn("Unable to update new workspace ACL")
            print("Updating ACL in a background thread")
            Thread(target=update_acl, daemon=True, name="ACL Update").start()
            self.sync()
        return result

    def get_submission(self, submission_id, lapdog_only=False):
        """
        Gets submission metadata from a lapdog or firecloud submission
        """
        if submission_id.startswith('lapdog/'):
            ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
            return WorkspaceManager(ns, ws).get_submission(sid)
        elif lapdog_id_pattern.match(submission_id):
            try:
                adapter = self.get_adapter(submission_id)
                if not adapter.live:
                    self._submission_cache[submission_id] = adapter.data
                return adapter.data
            except Exception as e:
                if lapdog_only:
                    raise NoSuchSubmission(submission_id) from e
                with dalmatian_api():
                    return super().get_submission(submission_id)
        if lapdog_only:
            raise NoSuchSubmission(submission_id)
        with dalmatian_api():
            return super().get_submission(submission_id)

    @parallelize(5)
    def _get_multiple_executions(self, execution_path):
        result = lapdog_submission_pattern.match(execution_path)
        if result:
            return self.get_submission(result.group(1), True)

    def get_adapter(self, submission_id):
        """
        Returns a submission adapter for a lapdog submission
        """
        if submission_id.startswith('lapdog/'):
            return get_adapter(submission_id)
        return adapters.SubmissionAdapter(self.get_bucket_id(), submission_id, self.gateway)

    def list_submissions(self, config=None, lapdog_only=False, cached=False):
        """
        Lists submissions in the workspace
        """
        if cached:
            return [*self._submission_cache.values()]
        results = []
        if not lapdog_only:
            with dalmatian_api():
                results = super().list_submissions(config)
        for submission in WorkspaceManager._get_multiple_executions(repeat(self), list_potential_submissions(self.bucket_id)):
            if submission is not None:
                results.append(submission)
        return results

    def execute(self, config_name, entity, expression=None, etype=None, force=False, memory=3, batch_limit=None, query_limit=None, offline_threshold=1000, private=False, region=None):
        """
        Validates config parameters then executes a job directly on GCP
        Config name may either be a full slug (config namespace/config name)
        or just the name (only if the name is unique)

        If memory is None (default): The cromwell VM will be an n1-standard-1
        Otherwise: The cromwell VM will be a custom instance with 2 CPU and the requested memory

        If batch_limit is None (default): The cromwell VM will run a maximum of 250 workflows per 3 GB on the cromwell instance
        Otherwise: The cromwell VM will run, at most, batch_limit concurrent workflows

        If query_limit is None (default): The cromwell VM will submit and query 100 workflows at a time.
        This has no bearing on the number of running workflows, but does slow the rate at which
        workflows get started and that failures are detected
        Otherwise: The cromwell VM will submit/query the given number of workflows at a time

        If private is False (default): The Cromwell VM and workers will have full access to the internet,
        but will count towards IP-address quotas. If set to True, Cromwell and workers can only access
        Google services, but do not count towards IP-address quotas
        """

        preflight = self.preflight(
            config_name,
            entity,
            expression,
            etype
        )

        if not preflight.result:
            raise ValueError(preflight.reason)

        if len(preflight.invalid_inputs):
            if not force:
                raise ValueError("The following inputs are invalid on this configuation: %s" % repr(list(preflight.invalid_inputs)))
            else:
                print("The following inputs are invalid on this configuation: %s" % repr(list(preflight.invalid_inputs)), file=sys.stderr)

        if not self.gateway.exists:
            raise ValueError("The Gateway for this Namespace has not been initialized")
        if not self.gateway.registered:
            raise ValueError("You are not registered with this Gateway. Please run WorkspaceManager.gateway.register()")

        submission_id = md5((gethostname() + str(time.time()) + preflight.config['namespace'] + preflight.config['name'] + preflight.entity).encode()).hexdigest()

        submission_data_path = os.path.join(
            'gs://'+self.get_bucket_id(),
            'lapdog-executions',
            submission_id,
            'submission.json'
        )
        blob = getblob(submission_data_path)
        while blob.exists():
            print("Submission ID collision detected. Generating a new ID...", file=sys.stderr)
            submission_id = md5((submission_id + str(time.time())).encode()).hexdigest()

        global_id = 'lapdog/'+base64.b64encode(
            ('%s/%s/%s' % (self.namespace, self.workspace, submission_id)).encode()
        ).decode()

        print("Global submission ID:", global_id)
        print("Workspace submission ID:", submission_id)

        print("This will launch", len(preflight.workflow_entities), "workflow(s)")

        if not force:
            print("Ready to launch workflow(s). Press Enter to continue")
            try:
                input()
            except KeyboardInterrupt:
                print("Aborted", file=sys.stderr)
                return

        if len(preflight.workflow_entities) > offline_threshold and self.live:
            resync = True
            print("This submission contains a large amount of workflows")
            print("Please wait while the workspace loads data to prepare the submission in offline mode...")
            self.populate_cache()
            print("Taking the cache offline...")
            self.go_offline()
        else:
            resync = False

        compute_regions = self.gateway.compute_regions
        if region is None:
            region = compute_regions[0]
        elif region not in compute_regions:
            raise NameError("Compute region %s not enabled for this namespace" % region)

        submission_data = {
            'workspace':self.workspace,
            'namespace':self.namespace,
            'identifier':global_id,
            'submission_id': submission_id,
            'methodConfigurationName':preflight.config['name'],
            'methodConfigurationNamespace':preflight.config['namespace'],
            'status': 'Running',
            'submissionDate': time.strftime(
                timestamp_format,
                time.gmtime()
            ),
            'submissionEntity': {
                'entityName': preflight.entity,
                'entityType': preflight.etype
            },
            'submitter': 'lapdog',
            'workflowEntityType': preflight.config['rootEntityType'],
            'workflowExpression': expression if expression is not None else None,
            'runtime': {
                'memory': memory,
                'batch_limit': (int(250*memory/3) if batch_limit is None else batch_limit),
                'query_limit': 100 if query_limit is None else query_limit,
                'private_access': private,
                'region': region
            }
        }

        try:
            config_types = {
                param['name']:{
                    'type': param['inputType'],
                    'required': not param['optional']
                }
                for param in getattr(fc, '__post')(
                    '/api/inputsOutputs',
                    data=json.dumps({
                        'methodNamespace': preflight.config['methodRepoMethod']['methodNamespace'],
                        'methodName': preflight.config['methodRepoMethod']['methodName'],
                        'methodVersion': preflight.config['methodRepoMethod']['methodVersion']
                    }),
                    timeout=2 # If it takes too long, just give up on typechecking
                ).json()['inputs']
            }
        except:
            traceback.print_exc()
            print("Warning: Firecloud request timed out. Preflight will not check data types", file=sys.stderr)
            config_types = {}

        @parallelize(5)
        def prepare_workflow(workflow_entity):
            wf_template = {}
            for k,v in preflight.config['inputs'].items():
                if len(v):
                    resolution = self.evaluate_expression(
                        preflight.config['rootEntityType'],
                        workflow_entity,
                        v
                    )
                    if k in config_types:
                        if config_types[k]['type'].startswith('Array'):
                            wf_template[k] = resolution
                        elif len(resolution) == 1:
                            wf_template[k] = resolution[0]
                        else:
                            raise ValueError("Unable to coerce array value {} to non-array parameter '{}' for entity '{}'".format(repr(resolution), k, workflow_entity))
                    else:
                        # We have no type info for this paramter, likely because the request failed
                        # Assume single-length values are values, and everything else is an array
                        if len(resolution) == 1:
                            wf_template[k] = resolution[0]
                        else:
                            wf_template[k] = resolution
            # Attempt robust preflight typecheck
            # Just check for missing required params
            for param, data in config_types.items():
                if data['required'] and param not in wf_template:
                    raise AttributeError("Parameter '{}' required but not defined for entity '{}'".format(param, workflow_entity))
                # if data['type'].startswith('Array') and not isinstance(wf_template[param], list):
                #     wf_template[param] = [wf_template[param]]
                #     warnings.warn("Coerced single-length ")
            return wf_template

        wdl_path = "gs://{bucket_id}/lapdog-executions/{submission_id}/method.wdl".format(
            bucket_id=self.get_bucket_id(),
            submission_id=submission_id
        )
        getblob(wdl_path).upload_from_string(
            self.get_wdl(
                preflight.config['methodRepoMethod']['methodNamespace'],
                preflight.config['methodRepoMethod']['methodName'],
                preflight.config['methodRepoMethod']['methodVersion']
            ).encode()
        )

        workflow_inputs = [*status_bar.iter(
            prepare_workflow(preflight.workflow_entities),
            len(preflight.workflow_entities),
            prepend="Preparing Workflows... "
        )]

        config_path = "gs://{bucket_id}/lapdog-executions/{submission_id}/config.tsv".format(
            bucket_id=self.get_bucket_id(),
            submission_id=submission_id
        )
        # AS OF newest lapdog, configs uploaded as TSV
        buff = StringIO()
        columns = [*{key for row in workflow_inputs for key in row}]
        writer = csv.DictWriter(
            buff,
            columns,
            delimiter='\t',
            lineterminator='\n'
        )
        writer.writeheader()
        for row in workflow_inputs:
            if len(json.dumps(row)) >= 10485760:
                raise ValueError("The size of input metadata cannot exceed 10 Mib for an individual workflow")
            writer.writerow(
                {
                    **{
                        column: None
                        for column in columns
                    },
                    **row
                }
            )
        getblob(config_path).upload_from_string(
            # json.dumps(workflow_inputs).encode()
            buff.getvalue().encode()
        )

        submission_data['workflows'] = [
            {
                'workflowEntity': e,
                'workflowOutputKey': build_input_key(t)
            }
            for e, t in zip(preflight.workflow_entities, workflow_inputs)
        ]

        blob.upload_from_string(json.dumps(submission_data))

        print("Connecting to Gateway to launch submission...")

        try:

            status, result = self.gateway.create_submission(
                self.workspace,
                self.bucket_id,
                submission_id,
                workflow_options={
                    'write_to_cache': True,
                    'read_from_cache': True,
                },
                memory=submission_data['runtime']['memory'],
                private=private,
                region=region
            )

            if not status:
                print("(%d)" % result.status_code, ":", result.text, file=sys.stderr)
                raise ValueError("Gateway failed to launch submission")

            print("Created submission", global_id)
            self._submission_cache[submission_id] = {
                **submission_data,
                **{'operation': result}
            }

            if self.initialize_hound() is not None:
                self.hound.write_log_entry(
                    'job',
                    (
                        "User started Lapdog Submission {};"
                        " Job results will be updated in hound when results are uploaded."
                        " Configuration: {}/{}, Entity: {}/{}, Expression: {},"
                        " Workflows: {}, Compute Region: {}, Private IP: {}"
                    ).format(
                        submission_id,
                        preflight.config['namespace'],
                        preflight.config['name'],
                        preflight.etype,
                        preflight.entity,
                        'null' if expression is None else expression,
                        len(preflight.workflow_entities),
                        region,
                        private
                    ),
                    entities=[os.path.join(preflight.etype, preflight.entity)]
                )

            if resync:
                print("Bringing the workspace back online")
                self.sync()

            return global_id, submission_id, result

        except:
            blob.delete()
            raise

    def get_submission_cost(self, submission_id):
        """
        Estimates the cost of a submission
        """
        if submission_id.startswith('lapdog/'):
            ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
            return WorkspaceManager(ns, ws).get_submission_cost(sid)
        elif lapdog_id_pattern.match(submission_id):
            return self.get_adapter(submission_id).cost()
        raise TypeError("get_submission_cost not available for firecloud submissions")

    def build_retry_set(self, submission_id):
        """
        Constructs a new entity_set of failures from a completed execution.
        """
        if submission_id.startswith('lapdog/'):
            ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
            return WorkspaceManager(ns, ws).build_retry_set(sid)
        elif not lapdog_id_pattern.match(submission_id):
            submission = self.get_submission(submission_id)
            workflowEntityType = submission['workflows'][0]['workflowEntity']['entityType']
            retries = [
                wf['workflowEntity']['entityName']
                for wf in submission['workflows']
                if wf['status'] == 'Failed'
            ]
            if len(retries) == 0:
                return None
            if len(retries) == 1:
                return {
                    'name': retries[0]
                }
            if workflowEntityType.endswith('_set'):
                raise TypeError("build_retry_set does not yet support super-sets")
            set_type = workflowEntityType+'_set'
            table = self._get_entities_internal(etype)
            name = submission['methodConfigurationName']+'_retries'
            if name in table.index:
                i = 2
                while '%s_%d' %(name, i) in table.index:
                    i += 1
                name = '%s_%d' %(name, i)
            with contextlib.ExitStack() as stack:
                if self.initialize_hound() is not None:
                    stack.enter_context(self.hound.with_reason("<Automated> Retrying submission {}".format(submission_id)))
                self.update_entity_set(workflowEntityType, name, retries)
            return {
                'name': name,
                'type': set_type,
                'expression': 'this.{}s'.format(workflowEntityType)
            }
        submission = self.get_adapter(submission_id)
        status = submission.status
        done = 'done' in status and status['done']
        if done:
            submission.update()
            retries = [
                wf['workflowEntity']
                for wf in submission.raw_workflows
                if wf['workflowOutputKey'] in submission.workflow_mapping and (
                    submission.workflows[submission.workflow_mapping[wf['workflowOutputKey']][:8]].status
                    if submission.workflow_mapping[wf['workflowOutputKey']][:8] in submission.workflows
                    else 'Starting'
                ) in {'Error', 'Failed'}
            ]
            if len(retries) == 0:
                return None
            if len(retries) == 1:
                return {
                    'name': retries[0]
                }
            if submission.data['workflowEntityType'].endswith('_set'):
                raise TypeError("build_retry_set does not yet support super-sets")
            set_type = submission.data['workflowEntityType']+'_set'
            table = self._get_entities_internal(set_type)
            name = submission.data['methodConfigurationName']+'_retries'
            if name in table.index:
                i = 2
                while '%s_%d' %(name, i) in table.index:
                    i += 1
                name = '%s_%d' %(name, i)
            with contextlib.ExitStack() as stack:
                if self.initialize_hound() is not None:
                    stack.enter_context(self.hound.with_reason("<Automated> Retrying submission {}".format(submission_id)))
                self.update_entity_set(submission.data['workflowEntityType'], name, retries)
            return {
                'name': name,
                'type': set_type,
                'expression': 'this.{}s'.format(submission.data['workflowEntityType'])
            }


    def submission_output_df(self, submission_id):
        """
        Checks a GCP job status and returns a dataframe of outputs
        """
        if submission_id.startswith('lapdog/'):
            ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
            return WorkspaceManager(ns, ws).submission_output_df(sid)
        elif lapdog_id_pattern.match(submission_id):
            submission = self.get_adapter(submission_id)
            status = submission.status
            done = 'done' in status and status['done']
            if done:
                output_template = self.get_config(
                    submission.data['methodConfigurationNamespace'],
                    submission.data['methodConfigurationName']
                )['outputs']

                output_data = {}
                try:
                    workflow_metadata = json.loads(getblob(
                        'gs://{bucket_id}/lapdog-executions/{submission_id}/results/workflows.json'.format(
                            bucket_id=self.get_bucket_id(),
                            submission_id=submission_id
                        )
                    ).download_as_string())
                except:
                    raise FileNotFoundError("Unable to locate the tracking file for this submission. It may not have finished")


                workflow_metadata = {
                    build_input_key(meta['workflow_metadata']['inputs']):meta
                    for meta in workflow_metadata
                    if meta['workflow_metadata'] is not None and 'inputs' in meta['workflow_metadata']
                }

                submission_workflows = {wf['workflowOutputKey']: wf['workflowEntity'] for wf in submission.data['workflows']}
                submission_data = pd.DataFrame()
                for key, entity in status_bar.iter(submission_workflows.items(), prepend="Processing Output... "):
                    if key not in workflow_metadata:
                        print("Entity", entity, "has no output metadata")
                    elif workflow_metadata[key]['workflow_status'] != 'Succeeded':
                        print("Entity", entity, "failed")
                        print("Errors:")
                        for call, calldata in workflow_metadata[key]['workflow_metadata']['calls'].items():
                            print("Call", call, "failed with error:", get_operation_status(calldata['jobId'])['error'])
                    else:
                        output_data = workflow_metadata[key]['workflow_output']
                        entity_data = pd.DataFrame(index=[entity])
                        for k,v in output_data['outputs'].items():
                            k = output_template[k]
                            if k.startswith('this.'):
                                entity_data[k[5:]] = [v]
                        submission_data = submission_data.append(entity_data, sort=True)
                return submission_data
        return pd.DataFrame()

    def complete_execution(self, submission_id):
        """
        Checks a GCP job status and returns results to firecloud, if possible
        """
        if submission_id.startswith('lapdog/'):
            ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
            return WorkspaceManager(ns, ws).complete_execution(sid)
        elif lapdog_id_pattern.match(submission_id):
            submission = self.get_adapter(submission_id)
            status = submission.status
            done = 'done' in status and status['done']
            if done:
                print("All workflows completed. Uploading results...")
                with contextlib.ExitStack() as stack:
                    if self.initialize_hound() is not None:
                        stack.enter_context(self.hound.with_reason('Uploading results from submission {}'.format(submission_id)))
                    self.update_entity_attributes(
                        submission.data['workflowEntityType'],
                        self.submission_output_df(submission_id)
                    )
                return True
            else:
                print("This submission has not finished")
                return False
        raise TypeError("complete_execution not available for firecloud submissions")

    def mop(self, *predicates, force=False, dry_run=False):
        """
        Cleans files from the workspace bucket which are not referenced by the data model.
        This is a VERY long operation.
        All files will be removed unless they meet any of the following conditions:
        1) The filepath is currently referenced by any entity or workspace attribute
        2) The filepath belongs to a lapdog submission and is a protected submission file
        3) The filepath belongs to a lapdog submission and the file has been modified since
        the last time any file was modified in the entity table for the entity type
        of this submission

        #1 protects from accidentally erasing live data
        #2 protects from accidentally removing key submission metadata
        #3 protects from accidentally removing files which *may* have been added to the data model
        since the WorkspaceManager was updated, and which may be waiting to be uploaded
        to FireCloud

        * You may specify arbitrary predicates which will be used to keep files.
        Each predicate is passed the blob object for a file being considered.
        If ANY user provided predicates return True, the file is KEPT.
        User predicates are run after the 3 built-in conditions. Files are only
        deleted if the file fails all 3 built-in conditions and all user provided
        conditions.
        * If force is True, this will not prompt for confirmation and will run in
        the background, returning a callback object
        * If dry_run is True, this will query and return the list of files that
        would be deleted, without actually deleting them

        If run in the foreground, this will return a tuple of the total size of
        freed in the bucket (as a string) and the list of files removed.
        If run in the background, this will return a callback object which returns
        the above tuple when the background function has finished
        """
        if not force:
            print("Warning: This operation may take a very long time to complete")
            print("Do you want to run this in the foreground?")
            print("Yes: Block until completion")
            print("No: Run in background (returns a calback object to wait for completion)")
            print("Ctrl+C: Abort")
            try:
                choice = input("Run in foreground? (y/N): ")
            except KeyboardInterrupt:
                print("Aborted")
                return
            force = len(choice) == 0 or not choice.lower().startswith('y')
        cb = self._mop(predicates, not force, dry_run)
        if not force:
            return cb()
        return cb

    @parallelize2()
    def _mop(self, predicates, fg=False, dry=False):
        """
        Background worker to mop a bucket
        """
        dest = sys.stdout if fg else open(os.devnull, 'w')
        cells = {}
        callbacks = []

        @parallelize2(10)
        def _get_cell_data(table, path):
            try:
                blob = strict_getblob(path)
                blob.reload()
                return (path, table, blob.time_created)
            except:
                pass

        def install_cell(table, path):
            if isinstance(path, str) and path.startswith('gs://'):
                callbacks.append(_get_cell_data(table, path))
            return path

        @lru_cache(256)
        def __get_adapter(sid):
            return self.get_adapter(sid)

        print("Loading Attributes", file=dest)
        for value in self.attributes.values():
            install_cell('attributes', value)
        modtime = {}
        for etype in self.entity_types:
            print("Loading", etype+'s', file=dest)
            self._get_entities_internal(etype).applymap(lambda cell:install_cell(etype, cell))
        for cb in status_bar.iter(callbacks, prepend="Scanning Data Model ", file=dest):
            try:
                entry = cb()
                if entry is not None:
                    cells[entry[0]] = (entry[1], entry[2])
            except:
                pass
        times = [cell[1] for cell in cells.values() if cell[0] == 'attributes']
        if len(times):
            modtime['attributes'] = sorted(times)[-1]
        for etype in self.entity_types:
            times = [cell[1] for cell in cells.values() if cell[0] == etype]
            if len(times):
                modtime[etype] = sorted(times)[-1]
        deleted = []
        size = 0
        time.sleep(10)
        print(cells)
        bucket_id = 'gs://'+self.bucket_id+'/'
        for page in storage.Client().bucket(self.bucket_id).list_blobs().pages:
            for blob in page:
                try:
                    if blob.exists():
                        if (bucket_id+blob.name) in cells:
                            # Referenced by data model
                            continue
                        protected = (
                            blob.name.endswith('submission.json') or
                            blob.name.endswith('results/workflows.json') or
                            blob.name.endswith('signature') or
                            blob.name.endswith('.log') or
                            blob.name.endswith('stdout') or
                            blob.name.endswith('stderr') or
                            blob.name.endswith('DO_NOT_DELETE_LAPDOG_WORKSPACE_SIGNATURE') or
                            blob.name.endswith('config.tsv') or
                            blob.name.startswith('hound')
                        )
                        if protected:
                            continue
                        blob.reload()
                        result = lapdog_submission_member_pattern.match(blob.name)
                        if result:
                            adapter = __get_adapter(result.group(1))
                            if adapter.live or blob.time_created is None or ('workflowEntityType' in adapter.data and adapter.data['workflowEntityType'] in modtime and modtime[adapter.data['workflowEntityType']] < blob.time_created):
                                # This blob is newer than the most recent cell in the entity table for the submission this blob belongs to
                                continue
                        keep = False
                        for condition in predicates:
                            if conditon(blob):
                                keep = True
                                break
                        if keep:
                            continue
                        print(bucket_id+blob.name, file=dest)
                        if not dry:
                            blob.delete()
                        size += blob.size
                        deleted.append(bucket_id+blob.name)
                except KeyboardInterrupt:
                    print("Aborted")
                    if not (dry or self.initialize_hound() is None):
                        self.hound.write_log_entry(
                            'other',
                            "Completed Workspace Mop. Freed size: {}".format(byteSize(size)),
                            entities=[deleted]
                        )
                    return deleted, byteSize(size)
                except:
                    traceback.print_exc()
        if not (dry or self.initialize_hound() is None):
            self.hound.write_log_entry(
                'other',
                "Completed Workspace Mop. Freed size: {}".format(byteSize(size)),
                entities=[deleted]
            )
        return deleted, byteSize(size)

    @_synchronized
    def get_method_version(self, namespace, name):
        # Offline wdl versions are a little complicated
        # Version -1 indicates a wdl which was uploaded offline, and should always be the priority
        if 'wdl:%s/%s.-1' % (namespace, name) in self.cache:
            return -1
        if self.live:
            # But if we're live, we can just query the latest version. Easy peasy
            try:
                with self.timeout(dog.DEFAULT_LONG_TIMEOUT):
                    return int(dog.get_method_version(namespace, name))
            except:
                self.go_offline()
        # However, if we're offline, or that fails, just pick the highest version number available in the offline cache
        versions = sorted(
            [k for k in self.cache if k.startswith('wdl:%s/%s.' % (namespace, name))],
            key=lambda x:int(x.split('.')[-1]),
            reverse=True
        )
        if len(versions):
            warnings.warn("This Workspace is offline. Version number may not reflect latest available version")
            return int(versions[0])
        # No offline versions. :(
        self.fail()

    @_synchronized
    @_read_from_cache(lambda self, namespace, name, version=None: 'wdl:{}/{}.{}'.format(namespace, name, version if version is not None else self.get_method_version(namespace, name)))
    def get_wdl(self, namespace, name, version=None):
        """
        Returns the WDL Text of the requested method
        """
        if version is None:
            version = self.get_method_version(namespace, name)
        response = fc.get_repository_method(namespace, name, version)
        if response.status_code == 404:
            raise NameError("No such wdl {}/{}@{}".format(namespace, name, version))
        return self.tentative_json(response)['payload']

    @_synchronized
    def upload_wdl(self, namespace, name, synopsis, path, delete=True):
        if self.live:
            try:
                with self.timeout(dog.DEFAULT_LONG_TIMEOUT):
                    dog.update_method(namespace, name, synopsis, path, delete_old=delete)
                    if self.initialize_hound() is not None:
                        self.hound.write_log_entry(
                            'other',
                            "Uploaded/Updated Method for workspace: {}/{}".format(
                                namespace,
                                name
                            )
                        )
                version = self.get_method_version(namespace, name)
                key = 'wdl:%s/%s.%d' % (namespace, name, version)
                with open(path) as r:
                    self.cache[key] = r.read()
                if 'wdl:%s/%s.-1' % (namespace, name) in self.cache:
                    # Once we make a successful upload, remove the offline cached WDL
                    # Otherwise the offline wdl would continue to supercede this one as the
                    # primary version
                    del self.cache['wdl:%s/%s.-1' % (namespace, name)]
                return version
            except ValueError:
                self.go_offline()
            except AssertionError:
                self.go_offline()
                print(
                    crayons.red("Warning:"),
                    "Unable to delete old snapshot. You must manually delete the existing wdl snapshot",
                    file=sys.stderr
                )
        print("Storing offline WDL in cache", file=sys.stderr)
        warnings.warn("WDL will be cached but not uploaded while offline. Manually re-upload after going live")
        key = 'wdl:%s/%s.-1' % (namespace, name)
        # Store wdl as version -1 since we can't lookup the version number
        with open(path) as r:
            self.cache[key] = r.read()
        return -1
