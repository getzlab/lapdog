from firecloud import api
from .schema import Evaluator
from functools import partial, wraps
from requests import Response, ReadTimeout
import sys
import crayons
import dalmatian as dog
from io import StringIO
import contextlib
import traceback
import pandas as pd
import threading
import time
import warnings

class APIException(ValueError):
    pass

timeout_state = threading.local()

DEFAULT_LONG_TIMEOUT = 30 # Seconds to wait if a value is not cached
DEFAULT_SHORT_TIMEOUT = 5 # Seconds to wait if a value is already cached

@contextlib.contextmanager
def set_timeout(n):
    try:
        if not hasattr(timeout_state, 'timeout'):
            timeout_state.timeout = None
        old_timeout = timeout_state.timeout
        timeout_state.timeout = n
        yield
    finally:
        timeout_state.timeout = old_timeout

getattr(api, "_fiss_agent_header")()
__CORE_SESSION_REQUEST__ = getattr(api, "__SESSION").request

def _firecloud_api_timeout_wrapper(*args, **kwargs):
    if not hasattr(timeout_state, 'timeout'):
        timeout_state.timeout = None
    return __CORE_SESSION_REQUEST__(
        *args,
        **{
            **{'timeout': timeout_state.timeout},
            **kwargs
        }
    )

getattr(api, "__SESSION").request = _firecloud_api_timeout_wrapper

@contextlib.contextmanager
def capture(display=True):
    try:
        stdout_buff = StringIO()
        stderr_buff = StringIO()
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = stdout_buff
        sys.stderr = stderr_buff
        yield (stdout_buff, stderr_buff)
    finally:
        sys.stdout = old_stdout
        stdout_buff.seek(0,0)
        sys.stderr = old_stderr
        stderr_buff.seek(0,0)
        if display:
            print(stderr_buff.read(), end='', file=sys.stderr)
            stderr_buff.seek(0,0)
            print(stdout_buff.read(), end='')
            stdout_buff.seek(0,0)

class Operator(object):
    """
    A cache layer between Lapdog/Dalmatian and Firecloud.
    Sits in between Lapdog and Dalmatian to serve/store data from/to an in-memory cache.
    In offline mode, and data updates are queued until the Operator goes online.
    The operator automatically switches offline if any errors are encountered with Firecloud
    """

    def __init__(self, workspace_manager):
        """
        Cache-enables a subset of firecloud operations
        Takes a lapdog workspace manager
        Integrates into Lapdog functionality so that it can run tasks
        while firecloud is offline
        """
        self.workspace = workspace_manager
        self.pending = []
        self.cache = {}
        self.dirty = set()
        self.live = True
        self.last_result = None
        self._webcache_ = False
        self._thread = None
        self.lock = threading.RLock()

    def __del__(self):
        try:
            self.go_live()
        except:
            traceback.print_exc()
        self.live = True # To halt background thread

    def __synchronized(func):
        # Terrible syntax but it works
        # During "compile time", while the class is being defined
        # __synchronized is not yet bound to any instances, so func refers to the
        # functions it wraps
        # When a new instance is created, func gets bound to 'self' and this
        # wrapper stops working right.
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            with self.lock:
                return func(self, *args, **kwargs)
        return wrapper

    def go_offline(self):
        self.live = False
        a, b, c = sys.exc_info()
        if a is not None and b is not None:
            traceback.print_exc()
        print(
            crayons.red("WARNING:", bold=False),
            "The operation cache is now offline for {}/{}".format(
                self.workspace.namespace,
                self.workspace.workspace
            )
        )
        with self.lock:
            if self._thread is None:
                self._thread = threading.Thread(
                    target=Operator.__resync_worker,
                    args=(self,),
                    name="Background synchronizer for {}/{}".format(
                        self.workspace.namespace,
                        self.workspace.workspace
                    ),
                    daemon=True
                )
                self._thread.start()

    def __resync_worker(self):
        time.sleep(60)
        while not self.live:
            try:
                with self.lock:
                    if len(self.pending):
                        print("Attempting to synchronize")
                        self.go_live()
                        self.live = False
            except:
                traceback.print_exc()
            time.sleep(60)
        with self.lock:
            self._thread = None # Delete it's own reference

    @__synchronized
    def go_live(self):
        failures = []
        exceptions = []
        for key, setter, getter in self.pending:
            try:
                if setter is not None:
                    response = setter()
                    if isinstance(response, Response) and response.status_code >=400:
                        raise APIException('API Failure: (%d) %s' % (response.status_code, response.text))
            except Exception as e:
                failures.append((key, setter, getter))
                exceptions.append(e)
                print(traceback.format_exc())
            else:
                try:
                    if getter is not None:
                        response = getter()
                        if isinstance(response, Response):
                            if response.status_code >=400:
                                raise APIException('API Failure: (%d) %s' % (response.status_code, response.text))
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
        self.pending = [item for item in failures]
        self.live = not len(self.pending)
        return self.live, exceptions

    @__synchronized
    def tentative_json(self, result, *expected_failures):
        self.last_result = result
        if result.status_code >= 400 and result.status_code not in expected_failures:
            self.go_offline()
            return None
        try:
            return result.json()
        except:
            self.go_offline()
            return None

    def timeout_for_key(self, key):
        if isinstance(key, str):
            return DEFAULT_SHORT_TIMEOUT if key in self.cache and self.cache[key] is not None else DEFAULT_LONG_TIMEOUT
        return key

    @contextlib.contextmanager
    def timeout(self, key):
        try:
            with set_timeout(self.timeout_for_key(key)):
                yield
        except ReadTimeout:
            traceback.print_exc()
            self.go_offline()

    def call_with_timeout(self, key, func, *args, **kwargs):
        try:
            with set_timeout(self.timeout_for_key(key)):
                return func(*args, **kwargs)
        except ReadTimeout:
            self.go_offline()
            # Do not silence the exception
            # call_with_timeout is used for background calls
            # don't want to accidentally fill the cache with Nones
            raise

    def fail(self):
        raise APIException(
            "Insufficient data in cache to complete operation. Last api result: (%d) %s" % (
                self.last_result.status_code if self.last_result is not None else 0,
                self.last_result.text if self.last_result is not None else '<No api calls>'
            )
        )

    @property
    def bucket_id(self):
        ws = self.firecloud_workspace
        if 'workspace' in ws and 'bucketName' in ws['workspace']:
            return ws['workspace']['bucketName']
        self.fail()

    @property
    @__synchronized
    def firecloud_workspace(self):
        if self.live:
            with self.timeout('workspace'):
                result = self.tentative_json(api.get_workspace(
                    self.workspace.namespace,
                    self.workspace.workspace
                ))
                if result is not None:
                    self.cache['workspace'] = result
        if 'workspace' in self.cache and self.cache['workspace'] is not None:
            return self.cache['workspace']
        self.fail()

    @property
    @__synchronized
    def configs(self):
        if self.live:
            with self.timeout('configs'):
                result = self.tentative_json(
                    api.list_workspace_configs(
                        self.workspace.namespace,
                        self.workspace.workspace
                    )
                )
                if result is not None:
                    self.cache['configs'] = [item for item in result]
        if 'configs' in self.cache and self.cache['configs'] is not None:
            return self.cache['configs']
        self.fail()

    @__synchronized
    def get_config_detail(self, namespace, name):
        key = 'config:%s/%s' % (namespace, name)
        if self.live:
            with self.timeout(key):
                result = self.tentative_json(api.get_workspace_config(
                    self.workspace.namespace,
                    self.workspace.workspace,
                    namespace,
                    name
                ))
                if result is not None:
                    self.cache[key] = result
        if key in self.cache and self.cache[key] is not None:
            return self.cache[key]
        self.fail()

    @__synchronized
    def _upload_config(self, config):
        configs = self.configs
        if (config['namespace']+'/'+config['name']) not in ['%s/%s'%(m['namespace'], m['name']) for m in configs]:
            # configuration doesn't exist -> name, namespace specified in json_body
            r = api.create_workspace_config(self.workspace.namespace, self.workspace.workspace, config)
            if r.status_code==201:
                print('Successfully added configuration: {}'.format(config['name']))
                return True
            else:
                print(r.text)
        else:
            r = api.update_workspace_config(self.workspace.namespace, self.workspace.workspace,
                    config['namespace'], config['name'], config)
            if r.status_code==200:
                print('Successfully updated configuration {}/{}'.format(config['namespace'], config['name']))
                return True
            else:
                print(r.text)
        return False

    @__synchronized
    def add_config(self, config):
        key = 'config:%s/%s' % (config['namespace'], config['name'])
        if 'configs' in self.cache and self.cache['configs'] is not None:
            # self.cache[config['name']] = config
            self.cache['configs'] = [
                cfg for cfg in self.cache['configs']
                if not (cfg['namespace'] == config['namespace'] and cfg['name'] == config['name'])
            ] + [
                {
                    'methodRepoMethod': config['methodRepoMethod'],
                    'name': config['name'],
                    'namespace': config['namespace'],
                    'rootEntityType': config['rootEntityType']
                }
            ]
        else:
            self.cache['configs'] = [
                {
                    'methodRepoMethod': config['methodRepoMethod'],
                    'name': config['name'],
                    'namespace': config['namespace'],
                    'rootEntityType': config['rootEntityType']
                }
            ]
        self.cache[key] = config
        if config['methodRepoMethod']['methodVersion'] == -1:
            # Wdl was uploaded offline, so we really shouldn't upload this config
            # Just put it in the cache and make the user upload later
            warnings.warn("Not uploading configuration referencing offline WDL")
            return False
        if self.live:
            result = self._upload_config(config)
            if result:
                return result
            self.go_offline()
        self.pending.append((
            None,
            partial(self._upload_config, config),
            None
        ))
        return False

    @__synchronized
    def get_method_version(self, namespace, name):
        # Offline wdl versions are a little complicated
        # Version -1 indicates a wdl which was uploaded offline, and should always be the priority
        if 'wdl:%s/%s.-1' % (namespace, name) in self.cache:
            return -1
        if self.live:
            # But if we're live, we can just query the latest version. Easy peasy
            try:
                with self.timeout(DEFAULT_LONG_TIMEOUT):
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

    @__synchronized
    def get_wdl(self, namespace, name, version=None):
        if version is None:
            version = self.get_method_version(namespace, name)
        key = 'wdl:%s/%s.%d' % (namespace, name, version)
        if self.live:
            try:
                with self.timeout(key):
                    response = api.get_repository_method(namespace, name, version)
                    if response.status_code == 404:
                        raise NameError("No such wdl {}/{}@{}".format(namespace, name, version))
                    response = self.tentative_json(response)
                    if response is not None:
                        self.cache[key] = response['payload']
            except KeyError:
                self.go_offline()
            except AssertionError:
                self.go_offline()
        if key in self.cache and self.cache[key] is not None:
            return self.cache[key]
        self.fail()

    @__synchronized
    def upload_wdl(self, namespace, name, synopsis, path, delete=True):
        if self.live:
            try:
                with self.timeout(DEFAULT_LONG_TIMEOUT):
                    dog.update_method(namespace, name, synopsis, path, delete_old=delete)
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

    @__synchronized
    def validate_config(self, namespace, name):
        cfgkey = 'config:%s/%s' % (namespace, name)
        key = 'valid:'+cfgkey
        if self.live:
            with self.timeout(key):
                result = self.tentative_json(api.validate_config(
                    self.workspace.namespace,
                    self.workspace.workspace,
                    namespace,
                    name
                ))
                if result is not None:
                    self.cache[key] = result
        if key in self.cache and self.cache[key] is not None and cfgkey not in self.dirty:
            return self.cache[key]
        print(
            crayons.red("WARNING:", bold=False),
            "This operator was unable to validate the config",
            file=sys.stderr
        )
        print("Assuming valid inputs and returning blank validation object")
        return {
            'invalidInputs': {},
            'missingInputs': []
        }

    @property
    @__synchronized
    def entity_types(self):
        if self.live:
            with self.timeout('entity_types'):
                result = self.tentative_json(
                    getattr(api, '__get')('/api/workspaces/{}/{}/entities'.format(
                        self.workspace.namespace,
                        self.workspace.workspace
                    ))
                )
                if result is not None:
                    self.cache['entity_types'] = result
        if 'entity_types' in self.cache and self.cache['entity_types'] is not None:
            return self.cache['entity_types']
        self.fail()

    @property
    @__synchronized
    def _entities_live_update(self):
        # Useful for force-updating the list of entity types, even if we're offline
        state = self.live
        self.live = True
        try:
            return self.entity_types
        finally:
            self.live = state

    @__synchronized
    def get_entities_df(self, etype):
        getter = partial(
            getattr(
                dog.WorkspaceManager,
                'get_'+etype+'s'
            ),
            self.workspace
        )
        key = 'entities:'+etype
        if self.live:
            try:
                with self.timeout(key):
                    df = getter()
                    self.cache[key] = df
            except AssertionError:
                self.go_offline()
            except TypeError:
                self.go_offline()
        if key in self.cache and self.cache[key] is not None:
            return self.cache[key]
        self.fail()

    @__synchronized
    def update_entities_df(self, etype, updates, index=True):
        getter = partial(
            self.call_with_timeout,
            DEFAULT_LONG_TIMEOUT,
            getattr(
                dog.WorkspaceManager,
                'get_'+etype+'s'
            ),
            self.workspace
        )
        # getter = getattr(
        #     self.workspace,
        #     'get_'+etype+'s'
        # )
        key = 'entities:'+etype
        if key not in self.cache or self.cache[key] is None:
            self.cache[key] = updates
        else:
            self.cache[key] = self.cache[key].append(
                updates.loc[[k for k in updates.index if k not in self.cache[key].index]]
            )
            self.cache[key].update(updates)
        self.dirty.add(key)
        if 'entity_types' not in self.cache:
            self.cache['entity_types'] = {}
        self.cache['entity_types'][etype] = {
            'attributeNames': [*self.cache[key].columns],
            'count': len(self.cache[key]),
            'idName': etype+'_id'
        }
        self.dirty.add('entity_types')
        if self.live:
            try:
                dog.WorkspaceManager.upload_entities(
                    self.workspace,
                    etype,
                    updates,
                    index
                )
                self.cache[key] = getter()
                if key in self.dirty:
                    self.dirty.remove(key)
            except ValueError:
                self.go_offline()
                self.pending.append((
                    key,
                    partial(dog.WorkspaceManager.upload_entities, self.workspace, etype, updates, index),
                    getter
                ))
                self.pending.append((
                    'entity_types',
                    None,
                    lambda x=None:self._entities_live_update
                ))
        else:
            self.pending.append((
                key,
                partial(dog.WorkspaceManager.upload_entities, self.workspace, etype, updates, index),
                getter
            ))
            self.pending.append((
                'entity_types',
                None,
                lambda x=None:self._entities_live_update
            ))
        if self.live:
            try:
                self.get_entities_df(etype)
            except APIException:
                pass

    @__synchronized
    def update_entities_df_attributes(self, etype, updates):
        getter = partial(
            self.call_with_timeout,
            DEFAULT_LONG_TIMEOUT,
            getattr(
                dog.WorkspaceManager,
                'get_'+etype+'s'
            ),
            self.workspace
        )
        # getter = getattr(
        #     self.workspace,
        #     'get_'+etype+'s'
        # )
        key = 'entities:'+etype
        if key not in self.cache or self.cache[key] is None:
            self.cache[key] = updates
        else:
            self.cache[key] = self.cache[key].append(
                updates.loc[[k for k in updates.index if k not in self.cache[key].index]]
            )
            self.cache[key].update(updates)
        self.dirty.add(key)
        if 'entity_types' not in self.cache:
            self.cache['entity_types'] = {}
        self.cache['entity_types'][etype] = {
            'attributeNames': [*self.cache[key].columns],
            'count': len(self.cache[key]),
            'idName': etype+'_id'
        }
        self.dirty.add('entity_types')
        if self.live:
            try:
                dog.WorkspaceManager.update_entity_attributes(
                    self.workspace,
                    etype,
                    updates
                )
                self.cache[key] = getter()
                if key in self.dirty:
                    self.dirty.remove(key)
            except ValueError:
                self.go_offline()
                self.pending.append((
                    key,
                    partial(dog.WorkspaceManager.update_entity_attributes, self.workspace, etype, updates),
                    getter
                ))
                self.pending.append((
                    'entity_types',
                    None,
                    lambda x=None:self._entities_live_update
                ))
        else:
            self.pending.append((
                key,
                partial(dog.WorkspaceManager.update_entity_attributes, self.workspace, etype, updates),
                getter
            ))
            self.pending.append((
                'entity_types',
                None,
                lambda x=None:self._entities_live_update
            ))
        if self.live:
            try:
                self.get_entities_df(etype)
            except APIException:
                pass

    @__synchronized
    def update_entity_set(self, etype, set_id, member_ids):
        key = 'entities:%s_set' % etype
        updates = pd.DataFrame(index=[set_id], data={etype+'s':[[*member_ids]]})
        if key not in self.cache or self.cache[key] is None:
            self.cache[key] = updates
        else:
            self.cache[key] = self.cache[key].append(
                updates.loc[[k for k in updates.index if k not in self.cache[key].index]],
                sort=True
            )
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
        setter = partial(
            dog.WorkspaceManager.update_entity_set,
            self.workspace,
            etype,
            set_id,
            member_ids
        )
        getter = partial(
            self.call_with_timeout,
            DEFAULT_LONG_TIMEOUT,
            getattr(
                dog.WorkspaceManager,
                'get_'+etype+'s'
            ),
            self.workspace
        )
        if self.live:
            try:
                with capture() as (stdout_buff, stderr_buff):
                    result = setter()
                    stdout_buff.seek(0,0)
                    stderr_buff.seek(0,0)
                    text = (stdout_buff.read() + stderr_buff.read()).lower()
                if not ('successfully imported' in text or 'successfully updated.' in text):
                    raise APIException("Update appeared to fail")
            except APIException:
                # One of the update routes failed
                self.go_offline()
        if not self.live:
            # offline. Add operations
            self.pending.append((
                key,
                setter,
                getter
            ))
            self.pending.append((
                'entity_types',
                None,
                lambda x=None:self._entities_live_update
            ))
        else:
            try:
                self.get_entities_df(etype+'_set')
            except APIException:
                pass


    @property
    def attributes(self):
        ws = self.firecloud_workspace
        if 'workspace' in ws and 'attributes' in ws['workspace']:
            return ws['workspace']['attributes']
        self.fail()

    @__synchronized
    def update_attributes(self, attrs):
        if 'workspace' in self.cache:
            if self.cache['workspace'] is None:
                self.cache['workspace'] = {
                    'workspace': {
                        'attributes': {k:v for k,v in attrs.items()}
                    }
                }
            else:
                self.cache['workspace']['workspace']['attributes'].update(attrs)
            self.dirty.add('workspace')
        if self.live:
            try:
                dog.WorkspaceManager.update_attributes(self.workspace, attrs)
            except AssertionError:
                self.go_offline()
                self.pending.append((
                    'workspace',
                    partial(dog.WorkspaceManager.update_attributes, self.workspace, attrs),
                    partial(self.call_with_timeout, DEFAULT_LONG_TIMEOUT, api.get_workspace, self.workspace.namespace, self.workspace.workspace)
                ))
        else:
            self.pending.append((
                'workspace',
                partial(dog.WorkspaceManager.update_attributes, self.workspace, attrs),
                partial(self.call_with_timeout, DEFAULT_LONG_TIMEOUT, api.get_workspace, self.workspace.namespace, self.workspace.workspace)
            ))
        if self.live:
            try:
                self.cache['workspace'] = api.get_workspace(self.workspace.namespace, self.workspace.workspace)
            except AssertionError:
                pass

    @__synchronized
    def evaluate_expression(self, etype, entity, expression):
        if self.live:
            with self.timeout(DEFAULT_LONG_TIMEOUT):
                result = self.tentative_json(
                    getattr(api, '__post')(
                        'workspaces/%s/%s/entities/%s/%s/evaluate' % (
                            self.workspace.namespace,
                            self.workspace.workspace,
                            etype,
                            entity
                        ),
                        data=expression
                    ),
                    400,
                    404
                )
                if result is not None:
                    return result
        evaluator = Evaluator(self.entity_types)
        for _etype, data in self.entity_types.items():
            evaluator.add_entities(
                _etype,
                self.get_entities_df(_etype)
            )
        if 'workspace' in expression:
            evaluator.add_attributes(
                self.attributes
            )
        return evaluator(etype, entity, expression)
