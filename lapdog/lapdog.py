import os
import json
import dalmatian as dog
import firecloud.api
from dalmatian import getblob, copyblob, moveblob, strict_getblob, ConfigNotFound, ConfigNotUnique
import contextlib
import csv
from google.cloud import storage
from agutil.parallel import parallelize, parallelize2
from agutil import status_bar, byteSize, cmd as execute_command
from threading import Lock, Thread, RLock
import sys
import re
import tempfile
import time
import subprocess
import requests
import fnmatch
from collections import namedtuple
from hashlib import md5
import base64
import yaml
from glob import glob, iglob
import crayons
from io import StringIO
from . import adapters
from .adapters import get_operation_status, mtypes, NoSuchSubmission, CommandReader, build_input_key
from .cache import cache_init, cache_path
from .cloud.utils import ld_acct_in_project
from .gateway import Gateway, creation_success_pattern, get_gcloud_account, get_application_default_account, capture, get_proxy_account
from itertools import repeat
import pandas as pd
from socket import gethostname
from math import ceil
from functools import wraps, lru_cache, partial
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

class AuthorizedDomainException(ValueError):
    pass

def list_potential_submissions(bucket_id):
    """
    Lists submission.json files found in a given bucket
    """
    for page in storage.Client().bucket(bucket_id).list_blobs(prefix="lapdog-executions", fields='items/name,nextPageToken').pages:
        for blob in page:
            if lapdog_submission_pattern.match(blob.name):
                yield 'gs://{}/{}'.format(bucket_id, blob.name)

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

def complete_execution(submission_id):
    """
    Checks a GCP job status and returns results to firecloud, if possible
    """
    if submission_id.startswith('lapdog/'):
        ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
        return WorkspaceManager("{}/{}".format(ns, ws)).complete_execution(sid)
    raise TypeError("Global complete_execution can only operate on lapdog global ids")

def get_submission(submission_id):
    """
    Gets submission metadata from a lapdog or firecloud submission
    """
    if submission_id.startswith('lapdog/'):
        ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
        return WorkspaceManager("{}/{}".format(ns, ws)).get_submission(sid)
    raise TypeError("Global get_submission can only operate on lapdog global ids")

def get_adapter(submission_id):
    """
    Gets adapter for a lapdog submission
    """
    if submission_id.startswith('lapdog/'):
        ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
        return WorkspaceManager("{}/{}".format(ns, ws)).get_adapter(sid)
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

# =============
# Preflight helper classes
# =============

PreflightFailure = namedtuple("PreflightFailure", ["result", "reason"])
PreflightSuccess = namedtuple(
    "PreflightSuccess",
    [
        "result",
        "config",
        "entity",
        "etype",
        "workflow_entities",
    ]
)


# =============
# Operator Cache Helper Decorators
# =============

def _synchronized(func):
    """
    Synchronizes access to the function using the instance's lock.
    Use if the function touches the operator cache
    """
    @wraps(func)
    def call_with_lock(self, *args, **kwargs):
        with self.lock:
            return func(self, *args, **kwargs)
    return call_with_lock

def _read_from_cache(key, message=None):
    """
    Decorator factory.
    Use to decorate a method which populates a cache value.
    The decorated function will only run if the cache is live.
    The decorated function should attempt to retrieve a live value and return it
    Afterwards, regardless of if the function succeeded, attempt to retrieve
    the cached value or fail.
    Optionally provide a failure message as a second argument

    This decorator factory allows the code to more represent the mechanism
    for a live update and removes the boilerplate of updating and reading from cache

    If the key is callable, call it on all the provided args and kwargs to generate a key

    Use if your function is a relatively straightforward getter. Just decorate with
    this and _synchronized, and then build your function to fetch a result from firecloud.
    Combine with tentative_json in your function for best results
    """

    def decorator(func):

        @wraps(func)
        def call_using_cache(self, *args, **kwargs):
            # First, if the key is callable, use it to get a string key
            if callable(key):
                _key = key(self, *args, **kwargs)
            else:
                _key = key
            # Next, if the workspace is live, attempt a live update
            if self.live:
                # Call with timeout
                try:
                    with self.timeout(_key):
                        result = func(self, *args, **kwargs)
                        if _key in self.dirty:
                            self.dirty.remove(_key)
                        self.cache[_key] = result
                except requests.ReadTimeout:
                    pass
                except dog.APIException as e:
                    self._last_result = e.response
            # Return the cached value, if present
            if _key in self.cache and self.cache[_key] is not None:
                if not self.live:
                    print(
                        crayons.red("WARNING:", bold=False),
                        "Returning cached result".format(
                            self.namespace,
                            self.workspace
                        ),
                        file=sys.stderr
                    )
                return self.cache[_key]
            self.fail(message) # Fail otherwise

        return call_using_cache

    return decorator

def call_with_context(ctx, func, *args, _context_callable=False, **kwargs):
    """
    Returns the value of func(*args, **kwargs) within the context
    Set '_context_callable=True' if your contextmanager needs to be called first
    """
    if _context_callable:
        ctx = ctx()
    with ctx:
        return func(*args, **kwargs)

def partial_with_hound_context(hound, func, *args, **kwargs):
    """
    Retuns a partially bound function
    Propagates the currently active hound reason (if any)
    Useful for capturing the current contextual hound reason when queueing a background action
    """
    if hound is not None:
        reason = hound.get_current_reason()
        return partial(
            call_with_context,
            partial(hound.with_reason, reason),
            func,
            *args,
            _context_callable=True,
            **kwargs
        )
    return partial(
        func,
        *args,
        **kwargs
    )

class WorkspaceManager(dog.WorkspaceManager):
    """
    Core Lapdog Class. Represents a single FireCloud workspace.
    Inherits from dalmatian.WorkspaceManager.
    Any features from dalmatian which are not explicitly upgraded by lapdog will
    still be accessible via inheritance
    """
    def __init__(self, reference, timezone='America/New_York', *, workspace_seed_url="http://localhost:4201"):
        """
        workspace_seed_url controls the url that will be queried to attempt to pre-populate the operator cache with data from
        a running lapdog UI. set to None to disable this feature
        Returns a Workspace object
        """
        super().__init__(
            reference,
            timezone
        )
        self.pending_operations = []
        self.cache = {}
        self.dirty = set()
        self.live = True
        self.lock = RLock()
        self._last_result = None
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
            bucket_id = self.get_bucket_id()
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
            traceback.print_exc()
            print("Warning: Unable to prepopulate workspace submission cache. Workspace may not exist", file=sys.stderr)
            self.sync()

    # ========================
    # Operator Cache Internals
    # ========================

    def go_offline(self):
        """
        Switches the WorkspaceManager into offline mode
        If there is a current exception being handled, log it
        """
        self.live = False
        a, b, c = sys.exc_info()
        if a is not None and b is not None:
            traceback.print_exc()
        print(
            crayons.red("WARNING:", bold=False),
            "The operation cache is now offline for {}/{}".format(
                self.namespace,
                self.workspace
            ),
            file=sys.stderr
        )

    @_synchronized
    def go_live(self):
        """
        Attempts to switch the WorkspaceManager into online mode
        Queued operations are replayed through the firecloud api
        If any operations fail, they are re-queued
        WorkspaceManager returns to online mode if all queued operations finish
        """
        failures = []
        exceptions = []
        for key, setter, getter in self.pending_operations:
            self.live = True # Always ensure live pathways are enabled during sync
            try:
                if setter is not None:
                    response = setter()
                    if isinstance(response, requests.Response) and response.status_code >= 400:
                        raise dog.APIException(r)
            except Exception as e:
                failures.append((key, setter, getter))
                exceptions.append(e)
                traceback.print_exc()
            else:
                try:
                    if getter is not None:
                        response = getter()
                        if isinstance(response, requests.Response):
                            if response.status_code >= 400:
                                raise dog.APIException(r)
                            else:
                                response = response.json()
                        if key is not None:
                            self.cache[key] = response
                            if key in self.dirty:
                                self.dirty.remove(key)
                except Exception as e:
                    failures.append((key, None, getter))
                    exceptions.append(e)
                    traceback.print_exc()
        self.pending_operations = [item for item in failures]
        self.live = not len(self.pending_operations)
        if len(exceptions):
            print("There were", len(exceptions), "exceptions while attempting to sync with firecloud")
        return self.live, exceptions

    sync = go_live

    def timeout_for_key(self, key):
        """
        Gets an appropriate request timeout based on a given cache key
        If the key is an integer, use it directly as the timeout
        """
        if isinstance(key, str):
            return dog.DEFAULT_SHORT_TIMEOUT if key in self.cache and self.cache[key] is not None else dog.DEFAULT_LONG_TIMEOUT
        return key

    @contextlib.contextmanager
    def timeout(self, key):
        """
        Context Manager: Temporarily sets the request timeout for this thread
        based on the given cache key/timeout value
        Useful for foreground calls
        """
        with dog.set_timeout(self.timeout_for_key(key)):
            yield

    def call_with_timeout(self, key, func, *args, **kwargs):
        """
        Calls the given function with the given arguments
        Applies a timeout based on the given cache key
        Useful for background calls
        """
        with dog.set_timeout(self.timeout_for_key(key)):
            return func(*args, **kwargs)

    def fail(self, message=None, offline=False):
        """
        Call when the WorkspaceManager cannot fulfil a user request.
        Raises an APIException based on the last request made.
        Optionally provide a message describing the failure.
        If offline is True, this will also switch to offline mode.
        """
        if message is None:
            message = ""
        if not self.live:
            message = "Workspace is offline and this request is not cached. Call 'WorkspaceManager.sync()' to go online. " + message
        elif offline:
            self.go_offline()
        if self._last_result is not None and self._last_result.status_code != 200:
            raise dog.APIException(message, self._last_result)
        raise dog.APIException(message)

    @contextlib.contextmanager
    def upload_context(self):
        """
        Good context to use when uploading data to firecloud
        Catches APIExceptions, and switches offline for 5XX errors
        Other errors are re-raised.
        Only use in synchronized functions
        """
        try:
            yield
        except dog.APIException as e:
            self._last_result = e.response
            if e.response.status_code >= 500:
                self.go_offline()
            else:
                raise

    def populate_cache(self):
        """
        Preloads all data from the FireCloud workspace into the in-memory cache.
        Use in advance of switching offline so that the WorkspaceManager can run in
        offline mode without issue.

        Call `WorkspaceManager.go_offline()` after this function to switch
        the workspace into offline mode
        """
        if self.live:
            self.sync()
        self.get_attributes()
        for etype in self.get_entity_types():
            self._get_entities_internal(etype)
        for config in self.list_configs():
            self.get_config(config)
            try:
                if 'methodRepoMethod' in config:
                    self.get_wdl(
                        config['methodRepoMethod']
                    )
            except NameError:
                # wdl not found
                pass
        self.sync()

    # ================================================
    # Workspace Getters and Internals
    # ================================================

    @_synchronized
    @_read_from_cache(lambda self, reference: 'config:{}'.format(reference))
    def get_config(self, reference):
        """
        Fetches a configuration by the provided reference
        Returns the configuration JSON object
        Accepts the following argument combinations
        1) reference = "name"
        2) reference = "namespace/name"
        """
        return super().get_config(reference)

    @_synchronized
    @_read_from_cache('entity_types')
    def get_entity_types(self):
        """
        Returns the different entity types present in the workspace
        Includes the count and column names of the entities
        """
        return super().get_entity_types()

    @_synchronized
    @_read_from_cache(lambda self, etype, page_size=1000: "entities:{}".format(etype))
    def get_entities(self, etype, page_size=1000):
        """
        Paginated query replacing get_entities_tsv()
        """
        # This method override is just to gain the operator cache's
        # synchonization and cacheing
        return super().get_entities(etype, page_size).copy()

    # This cache entry covers bucket_id and attributes
    @_synchronized
    @_read_from_cache('workspace')
    def get_workspace_metadata(self):
        """
        Get the full workspace entry from firecloud
        """
        return super().get_workspace_metadata()

    @_synchronized
    def _get_method_version_internal(self, repo, namespace, name):
        # Offline wdl versions are a little complicated
        # 1) Dockstore methods can't be uploaded in dalmatian, so they're not cached:
        if repo == 'dockstore':
            return dog.get_dockstore_method_version("dockstore.org/{}/{}".format(namespace, name))['name']
        # 2) If we uploaded a wdl in offline mode, it will be marked as -1
        # The offline WDL should always take priority
        identifier = '{}/{}'.format(namespace, name)
        if 'wdl:{}/-1'.format(identifier) in self.cache:
            return -1
        if self.live:
            # But if we're live, we can just query the latest version. Easy peasy
            try:
                with self.timeout(dog.DEFAULT_LONG_TIMEOUT):
                    return int(dog.get_method_version(identifier))
            except requests.ReadTimeout:
                pass
            except dog.APIException as e:
                # Save the _last_result here, which will make a failure message cleaner
                self._last_result = e.response
        # However, if we're offline, or that fails, just pick the highest version number available in the offline cache
        versions = sorted(
            [k for k in self.cache if k.startswith('wdl:{}/'.format(identifier))],
            key=lambda x:int(x.split('/')[-1]),
            reverse=True
        )
        if len(versions):
            warnings.warn("This Workspace is offline. Version number may not reflect latest available version")
            return int(versions[0].split('/')[-1])
        # No offline versions. :(
        self.fail("Unable to determine latest method version: {}".format(identifier))

    @_synchronized
    @_read_from_cache(lambda self, qualified_reference: 'wdl:{}'.format(qualified_reference))
    def _get_wdl_internal(self, qualified_reference):
        """
        Returns the WDL Text of the requested method
        qualified_reference is expected to be a fully qualified method reference
        """
        return dog.get_wdl(qualified_reference)

    def get_wdl(self, reference):
        """
        Returns the WDL Text of the requested method
        Accepts following argument types:
        1) reference = "namespace/name"
        2) reference = "namespace/name/version"
        3) reference = "dockstore.org/repository/workflow"
        4) reference = "dockstore.org/repository/workflow/version"
        """
        # 1) Make sure a version is present for both agora and dockstore methods
        if reference.startswith('dockstore.org'):
            data = reference.split('/')
            if len(data) == 3:
                # Must get latest version
                data.append(self._get_method_version_internal('dockstore', data[1], data[2]))
            # Not an elif. Make sure we now have exactly 4 components
            if len(data) != 4:
                raise TypeError("Method reference in invalid format: {}".format(reference))
        else:
            data = reference.split('/')
            if len(data) == 2:
                # Must get latest version
                data.append(self._get_method_version_internal('agora', data[0], data[1]))
            # Not an elif. Make sure we now have exactly 4 components
            if len(data) != 3:
                raise TypeError("Method reference in invalid format: {}".format(reference))
        return self._get_wdl_internal('/'.join(str(component) for component in data))

    @_synchronized
    @_read_from_cache('configs')
    def list_configs(self):
        """
        List configurations in workspace
        """
        return super().list_configs(include_dockstore=True)

    # ================================================
    # Workspace Setters and Internals
    # ================================================


    @_synchronized
    def update_config(self, config, wdl=None, synopsis=None):
        """
        Create or update a method configuration (separate API calls)

        config = {
           'namespace': config_namespace,
           'name': config_name,
           'rootEntityType' : entity,
           'methodRepoMethod': {'methodName':method_name, 'methodNamespace':method_namespace, 'methodVersion':version},
           'methodNamespace': method_namespace,
           'inputs':  {},
           'outputs': {},
           'prerequisites': {},
           'deleted': False
        }

        Optionally, if wdl is not None, upload wdl as the latest version of the method.
        Method namespace and name are taken from config['methodRepoMethod']
        wdl may be a filepath or literal WDL text.

        If synopsis is None, a sensible default is used

        """
        if "namespace" not in config or "name" not in config:
            raise ValueError("Config missing required keys 'namespace' and 'name'")
        if 'sourceRepo' not in config['methodRepoMethod']:
            config['methodRepoMethod']['sourceRepo'] = 'agora'
        if wdl is not None:
            self.update_method(config['methodRepoMethod'], wdl, synopsis)
        if config['methodRepoMethod']['methodVersion'] == 'latest':
            # Autofill config version
            if config['methodRepoMethod']['sourceRepo'] == 'agora':
                config['methodRepoMethod']['methodVersion'] = self._get_method_version_internal(
                    'agora',
                    config['methodRepoMethod']['methodNamespace'],
                    config['methodRepoMethod']['methodName']
                )
            else:
                # Dockstore API is stable, so don't worry about caching dockstore lookup
                config['methodRepoMethod'] = dog.get_dockstore_method_version(
                    config['methodRepoMethod']['methodPath']
                )['methodRepoMethod']
        if self.live:
            if config['methodRepoMethod']['methodVersion'] == -1:
                # Wdl was uploaded offline, so we really shouldn't upload this config
                # Just put it in the cache and make the user upload later
                warnings.warn("Not uploading configuration referencing offline WDL")
            else:
                with self.upload_context():
                    super().update_config(config)
        identifier = '{}/{}'.format(config['namespace'], config['name'])
        key = 'config:' + identifier
        self.cache[key] = config # add full config object to cache
        if 'configs' not in self.cache:
            # Store the config listing in the cache too so it shows up for list_configs
            self.cache['configs'] = []
        self.dirty.add('configs')
        if identifier not in {'{}/{}'.format(c['namespace'], c['name']) for c in self.cache['configs']}:
            # New config
            # Append to configs cache entry since we know cache was just populated above
            self.cache['configs'].append(
                {
                    'methodRepoMethod': config['methodRepoMethod'],
                    'name': config['name'],
                    'namespace': config['namespace'],
                    'rootEntityType': config['rootEntityType']
                }
            )
        else:
            # update existing config
            self.cache['configs'] = [
                c for c in self.cache['configs']
                if '{}/{}'.format(c['namespace'], c['name']) != identifier
            ] + [
                {
                    'methodRepoMethod': config['methodRepoMethod'],
                    'name': config['name'],
                    'namespace': config['namespace'],
                    'rootEntityType': config['rootEntityType']
                }
            ]
        if not self.live:
            self.pending_operations.append((
                None, # Dont worry about a getter. The config entry is exactly as it will appear in FC
                partial_with_hound_context(self.hound, super().update_config, config),
                None
            ))
            self.pending_operations.append((
                'configs',
                None,
                super().get_configs
            ))


    @_synchronized
    def upload_entities(self, etype, df, index=True):
        """
        index: True if DataFrame index corresponds to ID
        """
        df = self.upload_entity_data(etype, df)
        getter = partial_with_hound_context(
            self.hound,
            self.call_with_timeout,
            dog.DEFAULT_LONG_TIMEOUT,
            super().get_entities,
            etype
        )
        key = 'entities:'+etype
        if self.live:
            # Try an upload here
            # If it doesn't work, switch offline
            with self.upload_context():
                super().upload_entities(etype, df, index)
        if index:
            if key not in self.cache or self.cache[key] is None:
                self.cache[key] = df
            else:
                # 1) Outer join using new columns only. This will add new rows and columns
                self.cache[key] = self.cache[key].join(
                    df[[col for col in df.columns if col not in self.cache[key].columns]],
                    how='outer'
                )
                # 2) Update. This will overrwrite existing columns that have new values
                self.cache[key].update(df)
            self.dirty.add(key)
            if 'entity_types' not in self.cache:
                self.cache['entity_types'] = {}
            self.cache['entity_types'][etype] = {
                'attributeNames': [*self.cache[key].columns],
                'count': len(self.cache[key]),
                'idName': etype+'_id'
            }
            self.dirty.add('entity_types')
        if not self.live:
            self.pending_operations.append((
                key,
                partial_with_hound_context(
                    self.hound,
                    super().upload_entities,
                    etype,
                    df,
                    index
                ),
                getter
            ))
            self.pending_operations.append((
                'entitiy_types',
                None,
                self.get_entity_types
            ))
        else:
            try:
                # Try to trigger an update
                self._get_entities_internal(etype)
            except dog.APIException:
                pass
        if not (self.live or index):
            warnings.warn("Entity may not be present in cache until next online sync")


    @_synchronized
    def update_entity_attributes(self, etype, attrs):
        """
        Create or update entity attributes

        attrs:
          pd.DataFrame: update entities x attributes
          pd.Series:    update attribute (attrs.name)
                        for multiple entities (attrs.index)

          To update multiple attributes for a single entity, use:
            pd.DataFrame(attr_dict, index=[entity_name]))

          To update a single attribute for a single entity, use:
            pd.Series({entity_name:attr_value}, name=attr_name)
        """
        # It's much cleaner if the lapdog code just handles dataframes,
        # So we'll transform a series to a dataframe here
        if isinstance(attrs, pd.Series):
            attrs = pd.DataFrame(attrs, columns=attrs.index.name)
            attrs.index.name = etype+'_id'
        elif not isinstance(attrs, pd.DataFrame):
            raise TypeError("update_entity_attributes only accepts pd.Series or pd.DataFrame")
        attrs = self.upload_entity_data(etype, attrs)
        getter = partial(
            self.call_with_timeout,
            dog.DEFAULT_LONG_TIMEOUT,
            super().get_entities,
            etype
        )
        if self.live:
            with self.upload_context():
                super().update_entity_attributes(etype, attrs)
        key = 'entities:'+etype
        if key not in self.cache or self.cache[key] is None:
            self.cache[key] = attrs
        else:
            if isinstance(attrs, pd.Series):
                df = pd.DataFrame(attrs).T
            else:
                df = attrs.copy()
            # 1) Outer join using new columns only. This will add new rows and columns
            self.cache[key] = self.cache[key].join(
                df[[col for col in df.columns if col not in self.cache[key].columns]],
                how='outer'
            )
            # 2) Update. This will overrwrite existing columns that have new values
            self.cache[key].update(df)
        self.dirty.add(key)
        if 'entity_types' not in self.cache:
            self.cache['entity_types'] = {}
        self.cache['entity_types'][etype] = {
            'attributeNames': [*self.cache[key].columns],
            'count': len(self.cache[key]),
            'idName': etype+'_id'
        }
        self.dirty.add('entity_types')
        if not self.live:
            # If we were already offline, or the attempted upload switched us offline:
            self.pending_operations.append((
                key,
                partial_with_hound_context(
                    self.hound,
                    self._df_upload_translation_layer,
                    super().update_entity_attributes,
                    etype,
                    attrs
                ),
                getter
            ))
            self.pending_operations.append((
                'entity_types',
                None,
                self.get_entity_types
            ))
        else:
            # If we're still online, attempt a fetch to confirm w/ firecloud
            try:
                self._get_entities_internal(etype)
            except dog.APIException:
                pass

    @_synchronized
    def update_entity_set(self, etype, set_id, member_ids):
        """Create or update an entity set"""
        setter = partial_with_hound_context(
            self.hound,
            super().update_entity_set,
            etype,
            set_id,
            member_ids
        )
        getter = partial(
            self.call_with_timeout,
            dog.DEFAULT_LONG_TIMEOUT,
            super().get_entities,
            etype+'_set'
        )
        # Note: under some conditions, super().update_entity_set may delegate to self.upload_entities
        # That's fine because under those conditions, index is set to False, so upload_entities
        # won't overwrite the cache
        if self.live:
            with self.upload_context():
                setter() #
        key = 'entities:%s_set' % etype
        updates = pd.DataFrame(index=pd.Index([set_id], name=etype+"_set_id"), data={etype+'s':[[*member_ids]]})
        if key not in self.cache or self.cache[key] is None:
            self.cache[key] = updates
        else:
            # 1) Outer join using new columns only. This will add new rows and columns
            self.cache[key] = self.cache[key].join(
                updates[[col for col in updates.columns if col not in self.cache[key].columns]],
                how='outer'
            )
            # 2) Update. This will overrwrite existing columns that have new values
            self.cache[key].update(updates)
        self.dirty.add(key)
        if 'entity_types' not in self.cache:
            self.cache['entity_types'] = {}
        self.cache['entity_types'][etype+'_set'] = {
            'attributeNames': [*self.cache[key].columns],
            'count': len(self.cache[key]),
            'idName': etype+'_set_id'
        }
        self.dirty.add('entity_types')
        if not self.live:
            # offline. Add operations
            self.pending_operations.append((
                key,
                setter,
                getter
            ))
            self.pending_operations.append((
                'entity_types',
                None,
                self.get_entity_types
            ))
        else:
            try:
                self._get_entities_internal(etype+'_set')
            except dog.APIException:
                pass

    @_synchronized
    def update_attributes(self, attr_dict=None, **kwargs):
        """
        Set or update workspace attributes. Wrapper for API 'set' call
        Accepts a dictionary of attribute:value pairs and/or keyword arguments.
        Updates workspace attributes using the combination of the attr_dict and any keyword arguments
        Any values which reference valid filepaths will be uploaded to the workspace
        """
        # First handle upload
        if attr_dict is None:
            attr_dict = {}
        attr_dict.update(kwargs)
        base_path = 'gs://{}/workspace'.format(self.get_bucket_id())
        uploads = []
        for key, value in attr_dict.items():
            if isinstance(value, str) and os.path.isfile(value):
                path = '{}/{}'.format(base_path, os.path.basename(value))
                uploads.append(dog.upload_to_blob(value, path))
                self.hound.write_log_entry(
                    'upload',
                    "Uploading new file to workspace: {} ({})".format(
                        os.path.basename(value),
                        byteSize(os.path.getsize(value))
                    ),
                    entities=['workspace.{}'.format(key)]
                )
                attr_dict[key] = path
        if len(uploads):
            [callback() for callback in status_bar.iter(uploads, prepend="Uploading attributes ")]

        # now cache and post to firecloud
        if self.live:
            with self.upload_context():
                super().update_attributes(attr_dict)
        if 'workspace' in self.cache:
            if self.cache['workspace'] is None:
                self.cache['workspace'] = {
                    'workspace': {
                        'attributes': {k:v for k,v in attr_dict.items()}
                    }
                }
            else:
                self.cache['workspace']['workspace']['attributes'].update(attr_dict)
            self.dirty.add('workspace')

        if not self.live:
            self.pending_operations.append((
                'workspace',
                partial_with_hound_context(
                    self.hound,
                    super().update_attributes,
                    attr_dict
                ),
                partial(self.call_with_timeout, dog.DEFAULT_LONG_TIMEOUT, firecloud.api.get_workspace, self.namespace, self.workspace)
            ))
        else:
            try:
                self.get_attributes()
            except dog.APIException:
                pass
        return attr_dict

    @_synchronized
    def upload_participants(self, participant_ids):
        """Upload a list of participants IDs"""
        if self.live:
            with self.upload_context():
                super().upload_participants(participant_ids)
        offline_df = pd.DataFrame(index=np.unique(participant_ids))
        offline_df.index.name = 'participant_id'
        key = 'entities:participant'
        if key not in self.cache or self.cache[key] is None:
            self.cache[key] = offline_df
        else:
            # 1) Outer join using new columns only. This will add new rows and columns
            self.cache[key] = self.cache[key].join(
                offline_df[[col for col in offline_df.columns if col not in self.cache[key].columns]],
                how='outer'
            )
            # 2) Update. This will overrwrite existing columns that have new values
            self.cache[key].update(offline_df)
        self.dirty.add(key)
        if 'entity_types' not in self.cache:
            self.cache['entity_types'] = {}
        self.cache['entity_types']['participant'] = {
            'attributeNames': [*self.cache[key].columns],
            'count': len(self.cache[key]),
            'idName': 'participant_id'
        }
        self.dirty.add('entity_types')
        if not self.live:
            self.pending_operations.append((
                key,
                partial_with_hound_context(self.hound, super().upload_participants, participant_ids),
                partial(self._get_entities_internal, 'participant')
            ))
            self.pending_operations.append((
                'entity_types',
                None,
                self.get_entity_types
            ))
        else:
            try:
                self._get_entities_internal('participant')
            except dog.APIException:
                pass


    @_synchronized
    def update_participant_entities(self, etype, target_set=None):
        """
        Attach entities (samples or pairs) to participants.
        If target_set is not None, only perform the update for samples/pairs
        belonging to the given set
        Parallelized update to run on 5 entities in parallel
        """
        if self.live:
            with self.upload_context():
                super().update_participant_entities(etype, target_set)
        if etype=='sample':
            df = self.get_samples()[['participant']]
        elif etype=='pair':
            df = self.get_pairs()[['participant']]
        else:
            raise ValueError('Entity type {} not supported'.format(etype))

        if target_set is not None:
            df = df.loc[
                df.index.intersection(
                    self._get_entities_internal(etype+'_set')[etype+'s'][target_set]
                )
            ]

        entities_dict = {k:g.index.values for k,g in df.groupby('participant')}
        participant_ids = np.unique(df['participant'])

        column = "{}s_{}".format(
            etype,
            (target_set if target_set is not None else '')
        )

        offline_df = pd.DataFrame(
            {column: [
                entities_dict[pid] for pid in participant_ids
            ]},
            index=participant_ids
        )
        offline_df.index.name = 'participant_id'

        # We can't just run update_participant_attributes, because if that goes through,
        # then we'll have broken attributes in Firecloud
        key = 'entities:participant'
        if key not in self.cache or self.cache[key] is None:
            self.cache[key] = offline_df
        else:
            # 1) Outer join using new columns only. This will add new rows and columns
            self.cache[key] = self.cache[key].join(
                df[[col for col in df.columns if col not in self.cache[key].columns]],
                how='outer'
            )
            # 2) Update. This will overrwrite existing columns that have new values
            self.cache[key].update(df)
        self.dirty.add(key)
        if 'entity_types' not in self.cache:
            self.cache['entity_types'] = {}
        self.cache['entity_types'][etype] = {
            'attributeNames': [*self.cache[key].columns],
            'count': len(self.cache[key]),
            'idName': etype+'_id'
        }
        self.dirty.add('entity_types')
        if not self.live:
            self.pending_operations.append((
                key,
                partial_with_hound_context(
                    self.hound,
                    super().update_participant_entities,
                    etype,
                    target_set
                ),
                partial(
                    self._get_entities_internal,
                    'participant'
                )
            ))
            self.pending_operations.append((
                'entity_types',
                None,
                self.get_entity_types
            ))
        else:
            try:
                self._get_entities_internal('participant')
            except dog.APIException:
                pass

    @_synchronized
    def upload_wdl(self, method, synopsis, path, delete=True):
        """
        Upload a new method to the repository
        This is a Workspace-level method because it writes to the workspace cache
        """
        if self.live:
            # Try live update first, because this effects the method version
            with self.upload_context():
                dog.update_method(method, synopsis, path, delete_old=delete)
                self.hound.write_log_entry(
                    'other',
                    "Uploaded/Updated Method for workspace: {}".format(
                        method
                    )
                )
            version = self.get_method_version('agora', *method.split('/'))
            key = 'wdl:%s/%d' % (method, version)
            with open(path) as r:
                self.cache[key] = r.read()
            if 'wdl:%s/-1' % (method) in self.cache:
                # Once we make a successful upload, remove the offline cached WDL
                # Otherwise the offline wdl would continue to supercede this one as the
                # primary version
                del self.cache['wdl:%s/-1' % (method)]
            return version
        print("Storing offline WDL in cache", file=sys.stderr)
        warnings.warn("WDL will be cached but not uploaded while offline. Manually re-upload after going live")
        key = 'wdl:%s/-1' % (method)
        # Store wdl as version -1 since we can't lookup the version number
        with open(path) as r:
            self.cache[key] = r.read()
        return -1

    # ================================
    # Workspace Deleters and Internals
    # ================================

    @_synchronized
    def delete_config(self, reference):
        """
        Delete workspace configuration
        reference may be either of the following
        1) reference = "namespace/name"
        2) reference = "name" (if name is unique)
        """
        cfg = self.get_config(reference)
        if self.live:
            with self.upload_context():
                super().delete_config(reference)
        identifier = '{}/{}'.format(cfg['namespace'], cfg['name'])
        key = 'config:' + identifier
        if key in self.cache:
            del self.cache[key]
        # Only continue to modify cache if successful or 5XX Error failure
        self.cache['configs'] = [*filter(
            lambda entry: not (entry['namespace'] == cfg['namespace'] and entry['name'] == cfg['name']),
            self.cache['configs']
        )]
        self.dirty.add('configs')
        if not self.live:
            self.pending_operations.append((
                None,
                partial(super().delete_config, cfg['namespace'], cfg['name']),
                None
            ))
            self.pending_operations.append((
                'configs',
                None,
                self.list_configs
            ))

    @_synchronized
    def delete_entity_attributes(self, etype, attrs, entity_id=None, delete_files=False, dry_run=False):
        """
        Delete entity attributes and (optionally) their associated data

        Examples

          To delete an attribute for all samples:
            samples_df = wm.get_samples()
            wm.delete_entity_attributes('sample', samples_df[attr_name])

          To delete multiple attributes a single sample:
            wm.delete_entity_attributes('sample', attributes_list, entity_id=sample_id)

        WARNING: This action is not reversible. Be careful!
        """
        # Since this is a deletion, try live deletion first
        # Only continue to modify cache if successful or 5XX Error failure
        with self.upload_context():
            super().delete_entity_attributes(etype, attrs, entity_id, delete_files, dry_run)
        # If we got here, either the deletion went okay, or upload_context silenced a 5XX Error
        key = 'entities:' + etype
        if key in self.cache and not dry_run:
            # only modify cache if it wasn't a dry run
            if isinstance(attrs, pd.DataFrame):
                # df version: kill all rowsXcolumns provided
                for name, row in attrs.iterrows():
                    if name in self.cache[key].index:
                        for attr, _ in row.iteritems():
                            if attr in self.cache[key].columns:
                                self.cache[key][attr][name] = np.nan
            elif isinstance(attrs, pd.Series):
                # series version, kill series named attr for all entities on index
                if attrs.name in self.cache[key].columns:
                    for name, _ in attrs.iteritems():
                        if name in self.cache[key].index:
                            self.cache[key][attrs.name][name] = np.nan
            elif isinstance(attrs, list):
                # list version, kill all attrs listed for provided entity
                if entity_id in self.cache[key].index:
                    for attr in attrs:
                        if attr in self.cache[key].columns:
                            self.cache[key][attrs.name][name] = np.nan
            # Now drop empty rows and columns
            self.cache[key] = self.cache[key].dropna('index', 'all').dropna('columns', 'all')
            self.dirty.add(key)
            if self.live:
                try:
                    self._get_entities_internal(etype)
                except dog.APIException:
                    pass
            else:
                self.pending_operations.append((
                    None,
                    partial_with_hound_context(
                        self.hound,
                        super().delete_entity_attributes,
                        etype, attrs, entity_id, delete_files, dry_run
                    ),
                    None
                ))

    @_synchronized
    def delete_entity(self, etype, entity_ids):
        """Delete entity or list of entities"""
        # Since this is a deletion, try live deletion first
        # Only continue to modify cache if successful or 5XX Error failure
        with self.upload_context():
            super().delete_entity(etype, entity_ids)
        # If we got here, either the deletion went okay, or upload_context silenced a 5XX Error
        key = 'entities:' + etype
        if key in self.cache:
            self.cache[key] = self.cache[key].drop(entity_ids, errors='ignore')
            self.dirty.add(key)
        if self.live:
            try:
                self._get_entities_internal(etype)
            except dog.APIException:
                pass
        else:
            self.pending_operations.append((
                None,
                partial_with_hound_context(
                    self.hound,
                    super().delete_entity,
                    etype, entity_ids
                ),
                None
            ))

    # NOTE: Intentionally not including delete_participant or delete_sample
    # These methods don't ever delegate to the base delete_entity, or update_entity_attributes
    # Instead, they make base calls to fiss, which won't be captured by our overloads


    # =========================
    # Base Workspace Operations
    # =========================

    def create_workspace(self, parent=None):
        """
        Creates the workspace.
        You may provide a WorkspaceManager as the parent to clone from
        """
        result = super().create_workspace(parent)
        if not (self.gateway.project is None or self.gateway.registered):
            print("Not registered with gateway... Pre-registering")
            self.gateway.register(self.workspace, self.get_bucket_id())
        if result:
            def update_acl():
                time.sleep(30)
                try:
                    with self.hound.with_reason('<Automated> Auto-add lapdog proxy-group to workspace'):
                        response = self.update_acl({
                            get_proxy_account(): 'WRITER'
                        })
                except:
                    traceback.print_exc()
                    warnings.warn("Unable to update new workspace ACL")
            print("Updating ACL in a background thread")
            Thread(target=update_acl, daemon=True, name="ACL Update").start()
            self.sync()
        return result

    def copy_data(self):
        """
        Copies all data files referenced by workspace or entity attributes into
        this workspace's bucket.

        This is only useful if this workspace references files from other buckets,
        such as if it was cloned or uses external references.
        """

        bucket_id = self.get_bucket_id()

        def copy_to_workspace(value):
            if isinstance(value, str) and value.startswith('gs://'):
                src = getblob(value)
                destpath = 'gs://{}/{}'.format(bucket_id, src.name)
                if src.bucket.name != bucket_id:
                    copyblob(src, destpath)
                    return destpath
            return value

        with self.hound.with_reason("<AUTOMATED>: Migrating files into workspace bucket"):
            attributes = self.get_attributes()
            updated_attributes = {
                key:copy_to_workspace(value) for key,value in attributes.items()
            }
            # To save avoid redundant updates, only post updated attributes
            self.update_attributes({
                key:value for key,value in updated_attributes.items()
                if value != attributes[key]
            })

            for etype in self.get_entity_types():
                entity_df = self._get_entities_internal(etype).dropna(axis='columns', how='all')
                updated_df = entity_df.copy().applymap(copy_to_workspace)
                update_mask = (entity_df == updated_df).all()
                # Here's some crazy pandas operations, but ultimately, it just
                # grabs columns with at least one changed entity
                self.update_entity_attributes(etype, updated_df[update_mask[~update_mask].index])

    def mop(self, dry_run=False, quiet=False, delete_patterns=None, retain_patterns=None):
        """
        Cleans the workspace bucket of any unused files. By default, mop() retains
        the following file types:
        * Any file referenced by a workspace or entity attribute
        * Lapdog submission files: lapdog-executions/*/{submission.json,signature,config.tsv,results/workflows.json,abort-key}
        * Log files: {*.log,rc,*rc.txt,stdout,stderr}
        * Workflow scripts: {script,exec.sh}
        * Hound files: hound/**
        * Call Cache: lapdog-call-cache.sql
        * Workspace Signature: DO_NOT_DELETE_LAPDOG_WORKSPACE_SIGNATURE

        You can include additional files to delete with delete_patterns = [list of globs].
        You can include additional files to keep with retain_patterns = [list of globs].
        NOTE: All patterns are first checked against the full blob path.
        Patterns without a / will also be checked just against the blob's basename
        """
        reserved_patterns = [
            'lapdog-executions/*/submission.json',
            'lapdog-executions/*/signature',
            'lapdog-executions/*/config.tsv',
            'lapdog-executions/*/results/workflows.json',
            'lapdog-executions/*/abort-key',
            'lapdog-executions/*/method.wdl',
            '*.log',
            'rc',
            '*rc.txt',
            'stdout',
            'stderr',
            'script',
            'output',
            'exec.sh',
            'hound/**',
            'lapdog-call-cache.sql',
            'DO_NOT_DELETE_LAPDOG_WORKSPACE_SIGNATURE'
        ]
        if delete_patterns is None:
            delete_patterns = []
        if retain_patterns is None:
            retain_patterns = []
        bucket_prefix = "gs://{}/".format(self.get_bucket_id())
        if not quiet:
            print("Loading list of referenced file paths...")
        referenced_files = {
            os.path.relpath(value, bucket_prefix) for value in self.get_attributes().values()
            if isinstance(value, str) and value.startswith(bucket_prefix)
        }

        def scan_entity_cell(value):
            if isinstance(value, str) and value.startswith(bucket_prefix):
                referenced_files.add(os.path.relpath(value, bucket_prefix))

        for etype in self.get_entity_types():
            self._get_entities_internal(etype).applymap(scan_entity_cell)

        deleted_count = 0
        deleted_size = 0
        deleted_files = []
        retained_count = 0
        retained_size = 0

        if not quiet:
            print("Scanning objects in bucket...")

        try:
            for page in dog.core._getblob_client(None).bucket(self.get_bucket_id()).list_blobs(fields='items/name,items/size,nextPageToken').pages:
                for blob in page:
                    do_continue = False
                    for pattern in reserved_patterns:
                        if fnmatch.fnmatch(blob.name, pattern) or ('/' not in pattern and fnmatch.fnmatch(os.path.basename(blob.name), pattern)):
                            retained_count += 1
                            retained_size += blob.size
                            do_continue = True
                            break
                    if do_continue:
                        continue
                    for pattern in delete_patterns:
                        if fnmatch.fnmatch(blob.name, pattern) or ('/' not in pattern and fnmatch.fnmatch(os.path.basename(blob.name), pattern)):
                            deleted_count += 1
                            deleted_size += blob.size
                            if not dry_run:
                                blob.delete()
                                deleted_files.append(blob.name)
                            if not quiet:
                                print("Delete", blob.name)
                            do_continue = True
                            break
                    if do_continue:
                        continue
                    for pattern in retain_patterns:
                        if fnmatch.fnmatch(blob.name, pattern) or ('/' not in pattern and fnmatch.fnmatch(os.path.basename(blob.name), pattern)):
                            retained_count += 1
                            retained_size += blob.size
                            do_continue = True
                            break
                    if not (do_continue or blob.name in referenced_files):
                        deleted_count += 1
                        deleted_size += blob.size
                        if not dry_run:
                            blob.delete()
                            deleted_files.append(blob.name)
                        if not quiet:
                            print("Delete", blob.name)
        except KeyboardInterrupt:
            print("Aborted operation")
        if not quiet:
            print("Deleted", deleted_count, "files (", byteSize(deleted_size), ")")
            print("Retained", retained_count, "files (", byteSize(retained_size), ")")
        if not dry_run:
            self.hound.write_log_entry(
                'other',
                'Mopped the workspace bucket. Deleted {} files ({}) : {}'.format(
                    deleted_count,
                    byteSize(deleted_size),
                    deleted_files
                )
            )
        return deleted_count, deleted_size, retained_count, retained_size




    # ===================================
    # Submission Management and Internals
    # ===================================


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
            results = super().list_submissions(config)
        for submission in WorkspaceManager._get_multiple_executions(repeat(self), list_potential_submissions(self.get_bucket_id())):
            if submission is not None:
                results.append(submission)
        return results

    def get_submission(self, submission_id, lapdog_only=False):
        """
        Gets submission metadata from a lapdog or firecloud submission
        """
        if submission_id.startswith('lapdog/'):
            ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
            return WorkspaceManager("{}/{}".format(ns, ws)).get_submission(sid)
        elif lapdog_id_pattern.match(submission_id):
            try:
                adapter = self.get_adapter(submission_id)
                if not adapter.live:
                    self._submission_cache[submission_id] = adapter.data
                return adapter.data
            except Exception as e:
                if lapdog_only:
                    raise NoSuchSubmission(submission_id) from e
                return super().get_submission(submission_id)
        if lapdog_only:
            raise NoSuchSubmission(submission_id)
        return super().get_submission(submission_id)


    def preflight(self, config_name, entity, expression=None, etype=None):
        """
        Verifies submission configuration.
        This is just a quick check that the entity type, name, and expression map to
        one or more valid entities of the same type as the config's rootEntityType

        Returns a namedtuple.
        If tuple.result is False, tuple.reason will explain why preflight failed
        If tuple.result is True, you can access the following attributes:
        * (.config): The method configuration object
        * (.entity): The submission entity
        * (.etype): The submission entity type (inferred from the configuration, if not provided)
        * (.workflow_entities): The list of entities for each workflow (from evaluating the expression, if provided)
        """
        config = self.get_config(config_name)
        if (expression is not None) ^ (etype is not None and etype != config['rootEntityType']):
            return PreflightFailure(False, "expression and etype must BOTH be None or a string value")
        if etype is None:
            etype = config['rootEntityType']
        entities = self._get_entities_internal(etype)
        if entity not in entities.index:
            return PreflightFailure(
                False,
                "No such %s '%s' in this workspace. Check your entity and entity type" % (
                    etype,
                    entity
                )
            )

        workflow_entities = self.get_evaluator(self.live)(
            etype,
            entity,
            (expression if expression is not None else 'this')+'.%s_id' % config['rootEntityType']
        )
        if isinstance(workflow_entities, dict) and 'statusCode' in workflow_entities and workflow_entities['statusCode'] >= 400:
            return PreflightFailure(False, workflow_entities['message'] if 'message' in workflow_entities else repr(workflow_entities))
        elif not len(workflow_entities):
            return PreflightFailure(False, "Expression evaluates to 0 entities")

        return PreflightSuccess(True, config, entity, etype, workflow_entities)



    def execute(self, config_name, entity, expression=None, etype=None, force=False, use_cache=True, memory=3, batch_limit=None, offline_threshold=100, private=False, region=None, _authdomain_parent=None):
        """
        Validates config parameters then executes a job directly on GCP
        Config name may either be a full slug (config namespace/config name)
        or just the name (only if the name is unique)

        If memory is None (default): The cromwell VM will be an n1-standard-1
        Otherwise: The cromwell VM will be a custom instance with 2 CPU and the requested memory

        If batch_limit is None (default): The cromwell VM will run a maximum of 250 workflows per 3 GB on the cromwell instance
        Otherwise: The cromwell VM will run, at most, batch_limit concurrent workflows

        If private is False (default): The Cromwell VM and workers will have full access to the internet,
        but will count towards IP-address quotas. If set to True, Cromwell and workers can only access
        Google services, but do not count towards IP-address quotas
        """
        # This is a long one, so it's divided into secions

        # ----------------------------------------------------------------------
        # 1) Preflight: Gather information and prepare basic submission object
        # ----------------------------------------------------------------------
        metadata = self.get_workspace_metadata()
        authdomain = False
        if 'authorizationDomain' in metadata['workspace'] and len(metadata['workspace']['authorizationDomain']):
            warnings.warn(
                "Workspace exists in authorized domain. Enabling workaround",
                stacklevel=2
            )
            authdomain = True

        preflight = self.preflight(
            config_name,
            entity,
            expression,
            etype
        )

        if not preflight.result:
            raise ValueError(preflight.reason)

        if not self.gateway.exists:
            raise ValueError("The Gateway for this Namespace has not been initialized")
        if not self.gateway.registered:
            raise ValueError("You are not registered with this Gateway. Please run WorkspaceManager.gateway.register()")

        compute_regions = self.gateway.compute_regions
        if region is None:
            region = compute_regions[0]
        elif region not in compute_regions:
            raise NameError("Compute region %s not enabled for this namespace" % region)

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
            submission_data_path = os.path.join(
                'gs://'+self.get_bucket_id(),
                'lapdog-executions',
                submission_id,
                'submission.json'
            )
            blob = getblob(submission_data_path)

        global_id = 'lapdog/'+base64.b64encode(
            ('%s/%s/%s' % (self.namespace, self.workspace, submission_id)).encode()
        ).decode()

        print("Global submission ID:", global_id)
        print("Workspace submission ID:", submission_id)

        print("This will launch", len(preflight.workflow_entities), "workflow(s)")

        if not (force or authdomain):
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

        min_memory = 3
        cache_size = 0
        if use_cache:
            cache_blob = getblob('gs://{}/lapdog-call-cache.sql'.format(self.get_bucket_id()))
            if cache_blob.exists():
                cache_blob.reload()
                cache_size = cache_blob.size
                min_memory = ceil(max(3, cache_size / 976128930)) # slightly less than 1Gib. Gives breathing room while scaling
                if min_memory > memory:
                    warnings.warn("Increasing cromwell memory to accomodate large call cache", stacklevel=2)

        submission_data = {
            'workspace':self.workspace,
            'namespace':self.namespace,
            'identifier':global_id,
            'submissionId': submission_id,
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
            'submitter': self.hound.author,
            'workflowEntityType': preflight.config['rootEntityType'],
            'workflowExpression': expression if expression is not None else None,
            'runtime': {
                'memory': max(memory, min_memory),
                'batch_limit': (int(500*memory) if batch_limit is None else batch_limit),
                'query_limit': 100,
                'private_access': private,
                'region': region,
                'callcache': use_cache
            }
        }

        # ----------------------------------------------------------------------
        # 2) Prepare Inputs: All expressions evaluated to get final inputs
        # ----------------------------------------------------------------------
        if _authdomain_parent:
            # If this is an authorized domain bypass, store that in the submission data
            submission_data['AUTHORIZED_DOMAIN'] = _authdomain_parent

        try:
            # Try to get the data-types for the configuration
            # This helps us catch errors earlier on
            config_types = {
                param['name']:{
                    'type': param['inputType'],
                    'required': not param['optional']
                }
                for param in getattr(firecloud.api, '__post')(
                    '/api/inputsOutputs',
                    data=json.dumps(preflight.config['methodRepoMethod']),
                    timeout=2 # If it takes too long, just give up on typechecking
                ).json()['inputs']
            }
        except:
            traceback.print_exc()
            print("Warning: Firecloud request timed out. Preflight will not check data types", file=sys.stderr)
            config_types = {}

        evaluator = self.get_evaluator(self.live)

        # Get workflow inputs in parallel
        @parallelize(5)
        def prepare_workflow(workflow_entity):
            wf_template = {}
            for k,v in preflight.config['inputs'].items():
                if len(v):
                    resolution = evaluator(
                        preflight.config['rootEntityType'],
                        workflow_entity,
                        v
                    )
                    if k in config_types:
                        if config_types[k]['type'].startswith('Array'):
                            wf_template[k] = resolution
                        elif len(resolution) == 1:
                            wf_template[k] = resolution[0]
                        elif config_types[k]['required'] or len(resolution) != 0:
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
            return wf_template

        workflow_inputs = [*status_bar.iter(
            prepare_workflow(preflight.workflow_entities),
            len(preflight.workflow_entities),
            prepend="Preparing Workflows... "
        )]

        if authdomain:
            # ------------------------------------------------------------------
            # Authorized domain bypass
            # ------------------------------------------------------------------
            # This part is super gross, but so far it's the best I have for getting
            # through authorized domains
            # Bypass Step 1) Find an available workspace name and create it
            authdomain_child = self # initial value
            # We're not using the full proxy group, just the account for the current gateway
            proxy_acct = ld_acct_in_project(get_application_default_account(), self.gateway.project)
            while True:
                # Just keep trying until we land on a workspace we can use
                authdomain_child = WorkspaceManager(
                    self.namespace,
                    'ld-auth-{}'.format(md5(authdomain_child.workspace.encode()).hexdigest())
                )
                try:
                    authdomain_child.get_workspace_metadata()
                except dog.APIException as e:
                    if e.status_code == 404:
                        # Does not exist or we just don't have permissions to see it
                        try:
                            authdomain_child.live = True
                            authdomain_child.create_workspace() # create_workspace will automatically update ACL
                            break
                        except dog.APIException as e:
                            if e.status_code == 409:
                                # Already exists, we just can't see it, so try again with a new name
                                continue
                            raise
                else:
                    # We were able to read from the workspace, let's try updating the ACL
                    try:
                        authdomain_child.update_acl({proxy_acct: 'WRITER'})
                        break
                    except dog.APIException as e:
                        if e.status_code not in {401, 403}:
                            # Some other non-permissions status
                            raise
                        # Dont' have permissions to modify ACL
                        # Unfortunately, we can't check the ACL without these
                        # permissions, either, so assume the proxy account can't write
            print("This workspace is an authorized domain")
            print("Lapdog will use bypass workspace:", authdomain_child.workspace)
            print("Job will run in that workspace, results will be written back here")
            if not force:
                print("Press Enter to continue")
                input()
            dest_bucket = authdomain_child.get_bucket_id()
            # Bypass Step 2) Copy all the parsed input data to the bypass workspace
            copied = set()
            copy_lock = Lock()
            def copy_to_bypass(cell):
                if isinstance(cell, str) and cell.startswith('gs://'):
                    src = getblob(cell)
                    destpath = 'gs://{}/{}'.format(dest_bucket, src.name)
                    if not (cell == destpath or destpath in copied):
                        copyblob(src, destpath)
                        time.sleep(0.5)
                        with copy_lock:
                            copied.add(destpath)
                    return destpath
                elif isinstance(cell, list):
                    return [
                        copy_to_bypass(elem)
                        for elem in cell
                    ]
                return cell
            # Now Fudge a datamodel using input names as column names
            # md5encoding is not user-readable, but easier than writing a safe_name() function for firecloud
            column_map = { # Mapping of workflow inputs -> column names
                column: '_'+md5(column.encode()).hexdigest()
                for row in workflow_inputs
                for column in row
            }
            # The fake entity data:
            bypass_data = pd.DataFrame(
                [
                    {
                        column_map[key]:val
                        for key,val in row.items()
                    }
                    for row in workflow_inputs
                ],
                index=pd.Index(preflight.workflow_entities, name='{}_id'.format(preflight.config['rootEntityType']))
            ).applymap(copy_to_bypass)

            # Some fixups we need to do before uploading
            # Mostly making sure that we're meeting data model requirements
            if preflight.config['rootEntityType'] == 'sample' and 'participant' not in bypass_data.columns:
                bypass_data['participant'] = ['fake_participant'] * len(bypass_data)
                authdomain_child.upload_participants(['fake_participant'])
            if preflight.config['rootEntityType'] == 'pair':
                authdomain_child.upload_participants(['fake_participant'])
                if 'participant' not in bypass_data.columns:
                    bypass_data['participant'] = ['fake_participant'] * len(bypass_data)
                if 'case_sample' not in bypass_data.columns:
                    bypass_data['case_sample'] = ['fake_case_sample'] * len(bypass_data)
                    authdomain_child.upload_samples(pd.DataFrame.from_dict(
                        {'participant_id': ['fake_participant']}
                    ).set_index(pd.Index(['case_sample'], name='sample_id')))
                if 'control_sample' not in bypass_data.columns:
                    bypass_data['control_sample'] = ['fake_control_sample'] * len(bypass_data)
                    authdomain_child.upload_samples(pd.DataFrame.from_dict(
                        {'participant_id': ['fake_participant']}
                    ).set_index(pd.Index(['control_sample'], name='sample_id')))
            # Now actually upload
            with authdomain_child.hound.with_reason('Bypassing authorized domain in {}/{}'.format(self.namespace, self.workspace)):
                authdomain_child.upload_entities(
                    preflight.config['rootEntityType'],
                    bypass_data
                )
                authdomain_child.update_entity_attributes(
                    preflight.config['rootEntityType'],
                    bypass_data[[col for col in bypass_data.columns if col not in {'participant', 'participant_id', 'case_sample', 'control_sample'}]]
                )
                set_id = 'tmp_authdomain_{}_{}'.format(
                    preflight.config['name'],
                    md5(repr(preflight.workflow_entities).encode()).hexdigest()[:4]
                )
                authdomain_child.update_entity_set(
                    preflight.config['rootEntityType'],
                    set_id,
                    preflight.workflow_entities
                )

            # Bypass Step 3) Rewrite the configuration to use the new column names
            # The whole reason that we have to do this is because we're doing a 1-level
            # input name <-> column name mapping. This avoids having to reconstruct
            # a complicated data model with multi level expressions in the config
            # (ie: this.get_samples().bam_file becomes this.41741d8d082b848b8aaf9a8787f8b812)
            self.hound.write_log_entry('job', "Forwarding job to authorized domain bypass: {}/{}".format(self.namespace, authdomain_child.workspace))
            cfg = {
                **preflight.config,
                **{
                    'inputs': {
                        key: 'this.{}'.format(column_map[key])
                        for key in preflight.config['inputs']
                        if key in column_map
                    }
                }
            }
            authdomain_child.update_config(cfg)
            # Bypass Step 4) Final: Launch submission in new workspace
            global_id, local_id, operation_id = authdomain_child.execute(
                cfg,
                set_id,
                'this.{}s'.format(preflight.config['rootEntityType']),
                preflight.config['rootEntityType']+'_set',
                force=force,
                use_cache=use_cache,
                memory=memory,
                batch_limit=batch_limit,
                private=private,
                region=region,
                _authdomain_parent='{}/{}'.format(self.namespace, self.workspace)
            )
            # Don't return the local_id here because it's useless in the context of the parent workspace
            return (global_id, None, operation_id)

        # ----------------------------------------------------------------------
        # 3) Upload Submission data (end of Authorized domain bypass section)
        # ----------------------------------------------------------------------

        wdl_path = "gs://{bucket_id}/lapdog-executions/{submission_id}/method.wdl".format(
            bucket_id=self.get_bucket_id(),
            submission_id=submission_id
        )
        getblob(wdl_path).upload_from_string(
            self.get_wdl(
                preflight.config['methodRepoMethod']['methodPath']
                if 'sourceRepo' in preflight.config['methodRepoMethod'] and preflight.config['methodRepoMethod']['sourceRepo'] == 'dockstore'
                else "{}/{}".format(preflight.config['methodRepoMethod']['methodNamespace'], preflight.config['methodRepoMethod']['methodName'])
            ).encode()
        )

        config_path = "gs://{bucket_id}/lapdog-executions/{submission_id}/config.tsv".format(
            bucket_id=self.get_bucket_id(),
            submission_id=submission_id
        )
        # We choose to upload as a TSV because the TSV can be iterated over
        # which allows us to load workflows in chunks server-side instead of all
        # at once
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
            # PAPIv2 requires that a workflow request payload cannot exceed 10Mib
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

        # ----------------------------------------------------------------------
        # 4) Launch the submission
        # ----------------------------------------------------------------------

        print("Connecting to Gateway to launch submission...")

        try:

            status, result = self.gateway.create_submission(
                self.workspace,
                self.get_bucket_id(),
                submission_id,
                workflow_options={
                    'write_to_cache': use_cache,
                    'read_from_cache': use_cache,
                },
                memory=submission_data['runtime']['memory'],
                private=private,
                region=region,
                use_cache=use_cache,
                _cache_size=cache_size
            )

            if not status:
                print("(%d)" % result.status_code, ":", result.text, file=sys.stderr)
                raise ValueError("Gateway failed to launch submission")

            print("Created submission", global_id)
            self._submission_cache[submission_id] = {
                **submission_data,
                **{'operation': result}
            }

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
            # Clean up the submission data file if the submission fails
            # The presence of submission.json indicates an actual submission that ran
            blob.delete()
            raise

    def get_submission_cost(self, submission_id):
        """
        Estimates the cost of a submission
        """
        if submission_id.startswith('lapdog/'):
            ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
            return WorkspaceManager("{}/{}".format(ns, ws)).get_submission_cost(sid)
        elif lapdog_id_pattern.match(submission_id):
            return self.get_adapter(submission_id).cost()
        raise TypeError("get_submission_cost not available for firecloud submissions")

    def build_retry_set(self, submission_id):
        """
        Constructs a new entity_set of failures from a completed execution.
        """
        if submission_id.startswith('lapdog/'):
            ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
            return WorkspaceManager("{}/{}".format(ns, ws)).build_retry_set(sid)
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
            with self.hound.with_reason("<Automated> Retrying submission {}".format(submission_id)):
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
            with self.hound.with_reason("<Automated> Retrying submission {}".format(submission_id)):
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
            return WorkspaceManager("{}/{}".format(ns, ws)).submission_output_df(sid)
        elif lapdog_id_pattern.match(submission_id):
            submission = self.get_adapter(submission_id)
            status = submission.status
            done = 'done' in status and status['done']
            if done:
                output_template = self.get_config(
                    "{}/{}".format(
                        submission.data['methodConfigurationNamespace'],
                        submission.data['methodConfigurationName']
                    )
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
            return WorkspaceManager("{}/{}".format(ns, ws)).complete_execution(sid)
        elif lapdog_id_pattern.match(submission_id):
            submission = self.get_adapter(submission_id)
            status = submission.status
            done = 'done' in status and status['done']
            if done:
                submission_outputs = self.submission_output_df(submission_id)
                print("All workflows completed. Uploading results...")
                if 'AUTHORIZED_DOMAIN' in submission.data and not submission.data['AUTHORIZED_DOMAIN'].endswith(self.workspace):
                    upload_target = WorkspaceManager(submission.data['AUTHORIZED_DOMAIN'])
                    print("Copying outputs from job to parent workspace")
                    src_bucket = self.get_bucket_id()
                    dest_bucket = upload_target.get_bucket_id()
                    subprocess.check_call(
                        'gsutil -m cp -r gs://{src_bucket}/lapdog-executions/{submission_id} gs://{dest_bucket}/lapdog-executions/{submission_id}'.format(
                            src_bucket=src_bucket,
                            submission_id=submission_id,
                            dest_bucket=dest_bucket
                        ),
                        shell=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                    submission_outputs = submission_outputs.applymap(
                        lambda cell: cell if not (isinstance(cell, str) and cell.startswith('gs://')) else cell.replace(src_bucket, dest_bucket, 1)
                    )
                    print("Cleaning bucket")
                    # bucket = storage.Client().bucket(self.get_bucket_id())
                    # bucket.delete_blobs(blob for blob in bucket.list_blobs(fields='items/name,nextPageToken').pages)
                    subprocess.check_call(
                        'gsutil -m rm "gs://{}/**"'.format(src_bucket),
                        shell=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                else:
                    upload_target = self
                with upload_target.hound.with_reason('Uploading results from submission {}'.format(submission_id)):
                    upload_target.update_entity_attributes(
                        submission.data['workflowEntityType'],
                        submission_outputs
                    )
                return True
            else:
                print("This submission has not finished")
                return False
        raise TypeError("complete_execution not available for firecloud submissions")
