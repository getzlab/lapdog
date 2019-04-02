import os
import json
import dalmatian as dog
from firecloud import api as fc
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
from .adapters import get_operation_status, mtypes, NoSuchSubmission, CommandReader, safe_getblob, build_input_key
from .cache import cache_init, cache_path
from .operations import APIException, Operator, capture, set_timeout
from .cloud.utils import getblob, proxy_group_for_user, copyblob, moveblob
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
provenance_submission_pattern = re.compile(r'gs://(.+)/lapdog-executions/([0-9a-f]{32})/workspace/(?:.+)/(.{36})/call-')
provenance_workspace_pattern = re.compile(r'gs://(.+)/workspace/')
provenance_data_pattern = re.compile(r'gs://(.+)/((?:sample|participant|pair)(?:_set)?s)/')

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
        raise APIException("The Firecloud API has returned an unknown failure condition") from e

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
        raise APIException("The Firecloud API has returned status %d : %s" % (result.status_code, result.text))
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


class BucketUploader(object):
    """
    Class for automating uploads to gcloud buckets
    """
    def __init__(self, bucket, prefix, key):
        """
        Constructor
        bucket : The bucket id (do not include "gs://")
        prefix : A fixed path within the bucket to store all uploaded files
        key : A dictionary key to access on each object to specify additional path components.
        Set as None to use the str value of the object
        """
        self.bucket = bucket
        self.prefix = prefix
        self.key = key

    def upload(self, path, parent):
        """
        Upload an arbitrary object. Result path becomes:
        gs://{self.bucket}/{self.prefix, is prefix is not empty}/{parent[self.key]}/{basename(path)}
        if self.key is None:
        gs://{self.bucket}/{self.prefix, is prefix is not empty}/{parent}/{basename(path)}

        Returns the final gsutil path and a callback to wait for the upload to finish
        """
        bucket_path = os.path.join(
            self.prefix,
            parent[self.key] if self.key is not None else parent,
            os.path.basename(path)
        )
        return (
            'gs://%s/%s' % (self.bucket.id, bucket_path),
            upload(self.bucket, bucket_path, path)
        )

    def upload_df(self, df):
        """
        Iterates over dataframe rows
        Every cell value which is a valid local filepath is uploaded to:
        gs://{self.bucket}/{self.prefix, if prefix is not empty}/{row[self.key]}/{basename(filepath)}

        Returns a new dataframe with any uplpoaded files replaced with gsutil paths, and an array of callbacks to wait for each upload
        """

        uploads = []

        def scan_row(row):
            for i, value in enumerate(row):
                if isinstance(value, str) and os.path.isfile(value):
                    path, callback = self.upload(value, str(row.name))
                    row.iloc[i] = path
                    uploads.append(callback)
            return row

        return df.apply(scan_row, axis='columns'), uploads

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

def provenance(data):
    """
    Gets the origin of the provided data.
    Data must be a string, or a pandas series, dataframe, or arbitrary iterable of strings.
    """
    if isinstance(data, pd.DataFrame):
        return data.applymap(provenance)
    if isinstance(data, pd.Series):
        return data.apply(provenance)
    if isinstance(data, str):
        result = provenance_submission_pattern.match(data)
        if result:
            return "Submission Workflow {}/{}".format(result.group(2), result.group(3))
        result = provenance_workspace_pattern.match(data)
        if result:
            return "User uploaded Workspace Data"
        result = provenance_data_pattern.match(data)
        if result:
            return "User uploaded {} Data".format(result.group(2))
        return "Unknown"
    try:
        return [provenance(element) for element in iter(data)]
    except TypeError as e:
        raise TypeError("Unable to infer provenance operation from type " + repr(type(data))) from e
    raise TypeError("Provenance accepts strings, or iterables and pandas objects of strings")


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
        if workspace is not None:
            namespace = reference
        elif '/' in reference:
            namespace, workspace = reference.split('/')
        else:
            raise ValueError("Invalid argument combination")
        super().__init__(
            namespace,
            workspace,
            timezone
        )
        self.operator = Operator(self)
        if workspace_seed_url is not None:
            try:
                self.operator.cache = {
                    key:pickle.loads(base64.b64decode(value.encode()))
                    for key, value in requests.get(
                        workspace_seed_url+"/api/v1/workspaces/{namespace}/{workspace}/cache/seed".format(
                            namespace=namespace,
                            workspace=workspace
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

    def __repr__(self):
        return "<lapdog.WorkspaceManager {}/{}>".format(self.namespace, self.workspace)

    @property
    def pending_operations(self):
        """
        Property. Returns the count of operations queued while the WorkspaceManager is offline
        """
        return len(self.operator.pending)

    @property
    def live(self):
        """
        Property. Returns True if the WorkspaceManager is currently in online mode
        """
        return self.operator.live

    def sync(self):
        """
        Synchronize the workspace with Firecloud
        Any data updates since the workspace went offline are pushed to Firecloud
        Any external updates to the workspace are pulled in from Firecloud
        """
        is_live, exceptions = self.operator.go_live()
        if len(exceptions):
            print("There were", len(exceptions), "exceptions while attempting to sync with firecloud")
        return is_live, exceptions

    def populate_cache(self):
        """
        Preloads all data from the FireCloud workspace into the in-memory cache.
        Use in advance of switching offline so that the WorkspaceManager can run in
        offline mode without issue.

        Call `WorkspaceManager.operator.go_offline()` after this function to switch
        the workspace into offline mode
        """
        if self.live:
            self.sync()
        self.get_attributes()
        for etype in self.operator.entity_types:
            self.operator.get_entities_df(etype)
        for config in self.list_configs():
            self.operator.get_config_detail(config['namespace'], config['name'])
            try:
                self.operator.get_wdl(
                    config['methodRepoMethod']['methodNamespace'],
                    config['methodRepoMethod']['methodName'],
                    config['methodRepoMethod']['methodVersion']
                )
            except NameError:
                # WDL Doesnt exist
                pass
        self.sync()

    def get_bucket_id(self):
        """
        Returns the bucket ID of the workspace.
        Also available as a property: `WorkspaceManager.bucket_id`
        """
        return self.operator.bucket_id

    bucket_id = property(get_bucket_id)

    @property
    def acl(self):
        """
        Returns the current FireCloud ACL settings for the workspace
        """
        with set_timeout(30):
            result = fc.get_workspace_acl(self.namespace, self.workspace)
        if result.status_code == 403:
            raise ValueError("User lacks sufficient permissions to get workspace ACL")
        elif result.status_code == 404:
            exc = NameError("The requested workspace does not exist")
            exc._request_text = result.text
            raise exc
        elif result.status_code >= 400:
            exc = APIException("The FireCloud API returned an unhandled status: %d" % result.status_code)
            exc._request_text = result.text
            raise exc
        return result.json()['acl']

    def update_acl(self, acl):
        """
        Sets the ACL. Provide a dictionary of email -> access level
        """
        with set_timeout(30):
            response = fc.update_workspace_acl(
                self.namespace,
                self.workspace,
                [
                    {
                        'email': email,
                        'accessLevel': level
                    }
                    for email, level in acl.items()
                ]
            )
        if response.status_code >= 400:
            exc = APIException("The FireCloud API returned an unhandled status: %d" % result.status_code)
            exc._request_text = result.text
            raise exc
        return response.json()

    def create_workspace(self, parent=None):
        """
        Creates the workspace.
        You may provide a WorkspaceManager as the parent to clone from
        """
        with capture() as (stdout, stderr):
            with dalmatian_api():
                super().create_workspace(parent)
            stdout.seek(0,0)
            stderr.seek(0,0)
            text = stdout.read() + stderr.read()
        if bool(creation_success_pattern.search(text)):

            def update_acl():
                time.sleep(30)
                try:
                    from .gateway import get_access_token, get_token_info
                    response = self.update_acl({
                        proxy_group_for_user(get_token_info(get_access_token())['email']): 'WRITER'
                    })
                except:
                    traceback.print_exc()
                    warnings.warn("Unable to update new workspace ACL")
            print("Updating ACL in a background thread")
            Thread(target=update_acl, daemon=True).start()
            self.sync()
            return True
        return False

    def get_samples(self):
        """
        Gets the dataframe of samples from the workspace
        Also available as a property: `WorkspaceManager.samples`
        """
        return self.operator.get_entities_df('sample')

    samples = property(get_samples)

    def get_participants(self):
        """
        Gets the dataframe of participants from the workspace
        Also available as a property: `WorkspaceManager.participants`
        """
        return self.operator.get_entities_df('participant')

    participants = property(get_participants)

    def get_pairs(self):
        """
        Gets the dataframe of pairs from the workspace
        Also available as a property: `WorkspaceManager.pairs`
        """
        return self.operator.get_entities_df('pair')

    pairs = property(get_pairs)

    def get_sample_sets(self):
        """
        Gets the dataframe of sample sets from the workspace
        Also available as a property: `WorkspaceManager.sample_sets`
        """
        return self.operator.get_entities_df('sample_set')

    sample_sets = property(get_sample_sets)

    def get_participant_sets(self):
        """
        Gets the dataframe of participant sets from the workspace
        Also available as a property: `WorkspaceManager.participant_sets`
        """
        return self.operator.get_entities_df('participant_set')

    participant_sets = property(get_participant_sets)

    def get_pair_sets(self):
        """
        Gets the dataframe of pair sets from the workspace
        Also available as a property: `WorkspaceManager.pair_sets`
        """
        return self.operator.get_entities_df('pair_set')

    pair_sets = property(get_pair_sets)

    def prepare_entity_df(self, etype, df):
        """
        Takes a dataframe of entity attributes.
        Uploads attributes which reference valid filepaths
        Returns a dataframe suitiable to upload
        """
        df, uploads = BucketUploader(
            storage.Client().get_bucket(self.get_bucket_id()),
            etype+'s',
            None
        ).upload_df(df)
        if len(uploads):
            _ = [callback() for callback in status_bar.iter(uploads, prepend="Uploading {} data ".format(etype))]
        return df

    def prepare_sample_df(self, df):
        """
        Takes a dataframe of sample attributes
        Uploads filepaths and returns a modified dataframe
        """
        return self.prepare_entity_df('sample', df)

    def prepare_pair_df(self, df):
        """
        Takes a dataframe of pair attributes
        Uploads filepaths and returns a modified dataframe
        """
        return self.prepare_entity_df('pair', df)

    def prepare_participant_df(self, df):
        """
        Takes a dataframe of sample attributes
        Uploads filepaths and returns a modified dataframe
        """
        return self.prepare_entity_df('participant', df)

    def prepare_sample_set_df(self, df):
        """
        Takes a dataframe of sample_set attributes
        Uploads filepaths and returns a modified dataframe
        """
        return self.prepare_entity_df('sample_set', df)

    def prepare_pair_set_df(self, df):
        """
        Takes a dataframe of pair_set attributes
        Uploads filepaths and returns a modified dataframe
        """
        return self.prepare_entity_df('pair_set', df)

    def prepare_participant_set_df(self, df):
        """
        Takes a dataframe of participant_set attributes
        Uploads filepaths and returns a modified dataframe
        """
        return self.prepare_entity_df('participant_set', df)

    def upload_entities(self, etype, df, index=True):
        """
        Upload/Update entities in the workspace.
        Use when adding new entities to the workspace
        """
        return self.operator.update_entities_df(etype, df, index)

    def update_entity_attributes(self, etype, df):
        """
        Update attributes of existing entities.
        Use when modifying attributes of existing entities.
        If the input data is not a pandas.DataFrame, this will not be passed through
        the operator cache
        """
        if isinstance(df, pd.DataFrame):
            return self.operator.update_entities_df_attributes(etype, df)
        else:
            return super().update_entity_attributes(etype, df)

    def update_configuration(self, config, wdl=None, name=None, namespace=None, delete_old=True):
        """
        Update a method configuration and (optionally) the WDL
        Must provide a properly formed configuration object.
        If a wdl is provided, it must be a filepath or file-like object
        If a wdl is provided, the method name is specified by the name parameter
        or methodRepoMethod.methodName of the config, if name is None
        If a wdl is provided, the method namespace is specified by the namespace parameter
        or methodRepoMethod.methodNamespace of the config, if the namespace is None
        If methodRepoMethod.methodVersion is set to 'latest' in the config, it will
        be set to the most recent version of the method
        """
        if namespace is not None:
            config['methodRepoMethod']['methodNamespace'] = namespace
        if name is not None:
            config['methodRepoMethod']['methodName'] = name
        version = None
        if wdl is not None:
            with dump_if_file(wdl) as wdl_path:
                with capture() as (stdout, stderr):
                    self.operator.upload_wdl(
                        config['methodRepoMethod']['methodNamespace'],
                        config['methodRepoMethod']['methodName'],
                        "Runs " + config['methodRepoMethod']['methodName'],
                        wdl_path,
                        delete_old
                    )
                    stdout.seek(0,0)
                    out_text = stdout.read()
            result = re.search(r'New SnapshotID: (\d+)', out_text)
            if result:
                version = int(result.group(1))
        if config['methodRepoMethod']['methodVersion'] == 'latest':
            if version is None:
                version = self.operator.get_method_version(
                    config['methodRepoMethod']['methodNamespace'],
                    config['methodRepoMethod']['methodName']
                )
            config['methodRepoMethod']['methodVersion'] = version
        return self.operator.add_config(config)

    @alias(update_configuration)
    def update_config(self):
        # alias for update_configuration
        pass


    def update_attributes(self, attr_dict=None, **attrs):
        """
        Accepts a dictionary of attribute:value pairs and/or keyword arguments.
        Updates workspace attributes using the combination of the attr_dict and any keyword arguments
        Any values which reference valid filepaths will be uploaded to the workspace
        """
        if attr_dict is None:
            attr_dict = {}
        attr_dict.update(attrs)
        uploader = BucketUploader(
            storage.Client().get_bucket(self.get_bucket_id()),
            'workspace',
            None
        )
        uploads = []
        for k in attrs:
            if isinstance(attrs[k], str) and os.path.isfile(attrs[k]):
                path, callback = uploader.upload(
                    attrs[k],
                    ''
                )
                attrs[k] = path
                uploads.append(callback)
        if len(uploads):
            _ = [callback() for callback in status_bar.iter(uploads)]
        self.operator.update_attributes(attrs)
        return attrs

    def get_attributes(self):
        """
        Returns a dictionary of workspace attributes.
        Also available as a property: `WorkspaceManager.attributes`
        """
        return self.operator.attributes

    attributes = property(get_attributes)

    def update_entity_set(self, etype, set_id, entity_ids):
        """
        Updates entity set membership
        """
        return self.operator.update_entity_set(etype, set_id, entity_ids)

    def update_participant_entities(self, etype, target_set=None):
        """
        Attach entities (samples or pairs) to participants.
        If target_set is not None, only perform the update for samples/pairs
        belonging to the given set
        Parallelized update to run on 5 entities in parallel
        """
        if not self.live:
            raise APIException("WorkspaceManager.update_participant_entities does not use the operations cache. Please sync the workspace")

        if etype=='sample':
            df = self.samples[['participant']]
        elif etype=='pair':
            df = self.pairs[['participant']]
        else:
            raise ValueError('Entity type {} not supported'.format(etype))

        if target_set is not None:
            df = df.loc[
                df.index.intersection(
                    self.operator.get_entities_df(etype+'_set')[etype+'s'][target_set]
                )
            ]

        entitites_dict = {k:g.index.values for k,g in df.groupby('participant')}
        participant_ids = np.unique(df['participant'])

        @parallelize(5)
        def update_participant(participant_id):
            attr_dict = {
                "{}s_{}".format(
                    etype,
                    (target_set if target_set is not None else '')
                ): {
                    "itemsType": "EntityReference",
                    "items": [{"entityType": etype, "entityName": i} for i in entitites_dict[participant_id]]
                }
            }
            attrs = [fc._attr_set(i,j) for i,j in attr_dict.items()]
            r = fc.update_entity(self.namespace, self.workspace, 'participant', participant_id, attrs)
            return participant_id, r.status_code

        n_participants = len(participant_ids)

        for attempt in range(3):
            retries = []

            for k, status in status_bar.iter(update_participant(participant_ids), len(participant_ids), prepend="Updating {}s for participants ".format(etype)):
                if status >= 400:
                    retries.append(k)

            if len(retries):
                if attempt < 2:
                    print("\nRetrying remaining", len(retries), "participants")
                    participant_ids = [item for item in retries]
                else:
                    print("\nThe following", len(retries), "participants could not be updated:", ', '.join(retries), file=sys.stderr)
                    raise APIException("{} participants could not be updated after 3 attempts".format(len(retries)))
            else:
                break

        print('\n    Finished attaching {}s to {} participants'.format(etype, n_participants))

    def create_submission(self, config_name, entity, expression=None, etype=None, use_cache=True):
        """
        Validates config parameters then creates a submission in Firecloud.
        Returns the submission id.
        This function does not use the Lapdog Engine, and instead submits a job
        through the FireCloud Rawls API. Use `WorkspaceManager.execute` to run
        jobs through the Lapdog Engine
        """

        """
        Verifies execution configuration.
        The first return value is always a boolean indicating if the input was valid or not.
        If the configuration is invalid there will only be 2 return values, and the second
        value will be the error message.
        If the configuration is valid, there will be 7 return values:
        * True
        * The basic method configuration object
        * The submission entity
        * The submission entity type (inferred from the configuration, if not provided)
        * The list of entities for each workflow (from evaluating the expression, if provided)
        * The input template from the method configuration
        * A dictonary of input-name : error, for any invalid inputs in the configuration
        """
        if not self.live:
            print("The workspace is currently in offline mode. Please call WorkspaceManager.sync() to reconnect to firecloud", file=sys.stderr)
            return
        result = self.execute_preflight(config_name, entity, expression, etype)
        if not result[0]:
            raise ValueError(result[1])
        result, config, entity, etype, workflow_entities, template, invalidInputs = result
        with capture() as (stdout, stderr):
            with dalmatian_api():
                super().create_submission(
                    config['namespace'],
                    config['name'],
                    entity,
                    etype,
                    expression=expression,
                    use_callcache=use_cache
                )
            stdout.seek(0,0)
            stdout_text = stdout.read()
            stderr.seek(0,0)
            stderr_text = stderr.read()
        result = re.search(
            r'Successfully created submission (.+)\.',
            stdout_text
        )
        if result is None:
            raise APIException("Unexpected response from dalmatian: "+stdout_text)
        return result.group(1)

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

    def list_configs(self):
        """
        Lists configurations in the workspace
        Also available as properties: `WorkspaceManager.configs` and `WorkspaceManager.configurations`
        """
        return self.operator.configs

    def fetch_config(self, config_slug):
        """
        Fetches a configuration by the provided slug (method_config_namespace/method_config_name).
        If the slug is just the config name, this returns a config
        with a matching name IFF the name is unique. If another config
        exists with the same name, this will fail.
        If the slug is a full slug (namespace/name) this will always return
        a matching config (slug uniqueness is enforced by firecloud)
        """
        configs = self.list_configs()
        candidates = [] # For configs just matching name
        for config in configs:
            if config_slug == '%s/%s' % (config['namespace'], config['name']):
                return config
            elif config_slug == config['name']:
                candidates.append(config)
        if len(candidates) == 1:
            return candidates[0]
        elif len(candidates) > 1:
            raise ConfigNotUnique('%d configs matching name "%s". Use a full config slug' % (len(candidates), config_slug))
        raise ConfigNotFound('No such config "%s"' % config_slug)


    configs = property(list_configs)
    configurations = configs

    def get_adapter(self, submission_id):
        """
        Returns a submission adapter for a lapdog submission
        """
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

    def execute_preflight(self, config_name, entity, expression=None, etype=None):
        """
        Verifies execution configuration.
        The first return value is always a boolean indicating if the input was valid or not.
        If the configuration is invalid there will only be 2 return values, and the second
        value will be the error message.
        If the configuration is valid, there will be 7 return values:
        * True
        * The basic method configuration object
        * The submission entity
        * The submission entity type (inferred from the configuration, if not provided)
        * The list of entities for each workflow (from evaluating the expression, if provided)
        * The input template from the method configuration
        * A dictonary of input-name : error, for any invalid inputs in the configuration
        """
        config = self.fetch_config(config_name)
        if (expression is not None) ^ (etype is not None and etype != config['rootEntityType']):
            return False, "expression and etype must BOTH be None or a string value"
        if etype is None:
            etype = config['rootEntityType']
        entities = self.operator.get_entities_df(etype)
        if entity not in entities.index:
            return False, "No such %s '%s' in this workspace. Check your entity and entity type" % (
                etype,
                entity
            )

        workflow_entities = self.operator.evaluate_expression(
            etype,
            entity,
            (expression if expression is not None else 'this')+'.%s_id' % config['rootEntityType']
        )
        if isinstance(workflow_entities, dict) and 'statusCode' in workflow_entities and workflow_entities['statusCode'] >= 400:
            return False, workflow_entities['message'] if 'message' in workflow_entities else repr(workflow_entities)
        elif not len(workflow_entities):
            return False, "Expression evaluates to 0 entities"

        template = self.operator.get_config_detail(
            config['namespace'],
            config['name']
        )['inputs']

        invalid_inputs = self.operator.validate_config(
            config['namespace'],
            config['name']
        )
        invalid_inputs = {**invalid_inputs['invalidInputs'], **{k:'N/A' for k in invalid_inputs['missingInputs']}}

        return True, config, entity, etype, workflow_entities, template, invalid_inputs

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

        preflight_result = self.execute_preflight(
            config_name,
            entity,
            expression,
            etype
        )

        if not preflight_result[0]:
            raise ValueError(preflight_result[1])

        result, config, entity, etype, workflow_entities, template, invalid_inputs = preflight_result

        if len(invalid_inputs):
            if not force:
                raise ValueError("The following inputs are invalid on this configuation: %s" % repr(list(invalid_inputs)))
            else:
                print("The following inputs are invalid on this configuation: %s" % repr(list(invalid_inputs)), file=sys.stderr)

        if not self.gateway.exists:
            raise ValueError("The Gateway for this Namespace has not been initialized")
        if not self.gateway.registered:
            raise ValueError("You are not registered with this Gateway. Please run WorkspaceManager.gateway.register()")

        submission_id = md5((gethostname() + str(time.time()) + config['namespace'] + config['name'] + entity).encode()).hexdigest()

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

        print("This will launch", len(workflow_entities), "workflow(s)")

        if not force:
            print("Ready to launch workflow(s). Press Enter to continue")
            try:
                input()
            except KeyboardInterrupt:
                print("Aborted", file=sys.stderr)
                return

        if len(workflow_entities) > offline_threshold and self.live:
            resync = True
            print("This submission contains a large amount of workflows")
            print("Please wait while the workspace loads data to prepare the submission in offline mode...")
            self.populate_cache()
            print("Taking the cache offline...")
            self.operator.go_offline()
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
            'methodConfigurationName':config['name'],
            'methodConfigurationNamespace':config['namespace'],
            'status': 'Running',
            'submissionDate': time.strftime(
                timestamp_format,
                time.gmtime()
            ),
            'submissionEntity': {
                'entityName': entity,
                'entityType': etype
            },
            'submitter': 'lapdog',
            'workflowEntityType': config['rootEntityType'],
            'workflowExpression': expression if expression is not None else None,
            'runtime': {
                'memory': memory,
                'batch_limit': (int(250*memory/3) if batch_limit is None else batch_limit),
                'query_limit': 100 if query_limit is None else query_limit,
                'private_access': private,
                'region': region
            }
        }

        @parallelize(5)
        def prepare_workflow(workflow_entity):
            wf_template = {}
            for k,v in template.items():
                if len(v):
                    resolution = self.operator.evaluate_expression(
                        config['rootEntityType'],
                        workflow_entity,
                        v
                    )
                    if len(resolution) == 1:
                        wf_template[k] = resolution[0]
                    else:
                        wf_template[k] = resolution
            return wf_template

        wdl_path = "gs://{bucket_id}/lapdog-executions/{submission_id}/method.wdl".format(
            bucket_id=self.get_bucket_id(),
            submission_id=submission_id
        )
        getblob(wdl_path).upload_from_string(
            self.operator.get_wdl(
                config['methodRepoMethod']['methodNamespace'],
                config['methodRepoMethod']['methodName'],
                config['methodRepoMethod']['methodVersion']
            ).encode()
        )

        workflow_inputs = [*status_bar.iter(
            prepare_workflow(workflow_entities),
            len(workflow_entities),
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
            for e, t in zip(workflow_entities, workflow_inputs)
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
            return WorkspaceManager(ns, ws).submission_output_df(sid)
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
            table = self.operator.get_entities_df(set_type)
            name = submission['methodConfigurationName']+'_retries'
            if name in table.index:
                i = 2
                while '%s_%d' %(name, i) in table.index:
                    i += 1
                name = '%s_%d' %(name, i)
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
            table = self.operator.get_entities_df(set_type)
            name = submission.data['methodConfigurationName']+'_retries'
            if name in table.index:
                i = 2
                while '%s_%d' %(name, i) in table.index:
                    i += 1
                name = '%s_%d' %(name, i)
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
                output_template = self.operator.get_config_detail(
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
                self.operator.update_entities_df_attributes(
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
                blob = safe_getblob(path)
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
        for etype in self.operator.entity_types:
            print("Loading", etype+'s', file=dest)
            self.operator.get_entities_df(etype).applymap(lambda cell:install_cell(etype, cell))
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
        for etype in self.operator.entity_types:
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
                            blob.name.endswith('config.tsv')
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
                    return deleted, byteSize(size)
                except:
                    traceback.print_exc()
        return deleted, byteSize(size)
