from firecloud import api
from .schema import Evaluator
from functools import partial
from requests import Response
import sys
import crayons
import dalmatian as dog
from io import StringIO
import contextlib

class APIException(ValueError):
    pass

@contextlib.contextmanager
def capture():
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
        print(stdout_buff.read(), end='')
        stdout_buff.seek(0,0)
        sys.stderr = old_stderr
        stderr_buff.seek(0,0)
        print(stderr_buff.read(), end='', file=sys.stderr)
        stderr_buff.seek(0,0)

class Operator(object):
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

    def go_offline(self):
        self.live = False
        print(
            crayons.red("WARNING:", bold=False),
            "The operation cache is now offline for {}/{}".format(
                self.workspace.namespace,
                self.workspace.workspace
            )
        )

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
        self.pending = [item for item in failures]
        self.live = not len(self.pending)
        return self.live, exceptions

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

    def fail(self):
        raise APIException(
            "Insufficient data in cache to complete operation. Last api result: (%d) %s" % (
                self.last_result.status_code if self.last_result is not None else 0,
                self.last_result.text if self.last_result is not None else '<No api calls>'
            )
        )

    @property
    def configs(self):
        if self.live:
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

    def get_config_detail(self, namespace, name):
        key = 'config:%s/%s' % (namespace, name)
        if self.live:
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

    def _upload_config(self, config):
        with capture() as (stdout, stderr):
            dog.WorkspaceManager.update_configuration(
                self.workspace,
                config
            )
            stdout.seek(0,0)
            stderr.seek(0,0)
            text = stdout.read() + stderr.read()
        return 'Successfully' in text and 'configuration' in text and config['name'] in text

    def add_config(self, config):
        if 'configs' in self.cache and self.cache['configs'] is not None:
            self.cache[config['name']] = config
        self.cache['config:%s/%s' % (config['namespace'], config['name'])] = config
        if self.live:
            result = self._upload_config(config)
            if result is False:
                self.go_offline()
                self.pending.append((
                    None,
                    partial(self._upload_config, config),
                    None
                ))
            return result
        else:
            self.pending.append((
                None,
                partial(self._upload_config, config),
                None
            ))
        return False

    def get_wdl(self, namespace, name, version=None):
        key = 'wdl:%s/%s' % (namespace, name)
        if version is not None:
            key += '.%d'%version
        if self.live:
            try:
                if version is None:
                    version = dog.get_method_version(namespace, name)
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

    def upload_wdl(self, namespace, name, synopsis, path):
        key = 'wdl:%s/%s' % (namespace, name)
        with open(path) as r:
            self.cache[key] = r.read()
        if self.live:
            try:
                dog.update_method(namespace, name, synopsis, path)
            except ValueError:
                self.go_offline()
                self.pending.append((
                    key,
                    partial(dog.update_method, namespace, name, synopsis, path),
                    partial(dog.get_wdl, namespace, name)
                ))
            except AssertionError:
                self.go_offline()
                print(
                    crayons.red("Warning:"),
                    "Unable to delete old snapshot. You must manually delete the existing wdl snapshot",
                    file=sys.stderr
                )
        else:
            self.pending.append((
                key,
                partial(dog.update_method, namespace, name, synopsis, path),
                partial(dog.get_wdl, namespace, name)
            ))

    def validate_config(self, namespace, name):
        cfgkey = 'config:%s/%s' % (namespace, name)
        key = 'valid:'+cfgkey
        if self.live:
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
    def entity_types(self):
        if self.live:
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
                df = getter()
                self.cache[key] = df
            except AssertionError:
                self.go_offline()
            except TypeError:
                self.go_offline()
        if key in self.cache and self.cache[key] is not None:
            return self.cache[key]
        self.fail()

    def update_entities_df(self, etype, updates, index=True):
        getter = partial(
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
        if key in self.cache:
            if self.cache[key] is None:
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
                    lambda x=None:self.entities
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
                lambda x=None:self.entities
            ))
        if self.live:
            try:
                self.get_entities_df(etype)
            except APIException:
                pass

    @property
    def attributes(self):
        if self.live:
            try:
                self.cache['attributes'] = dog.WorkspaceManager.get_attributes(self.workspace)
            except AssertionError:
                self.go_offline()
        if 'attributes' in self.cache and self.cache['attributes'] is not None:
            return self.cache['attributes']
        self.fail()

    def update_attributes(self, attrs):
        if 'attributes' in self.cache:
            if self.cache['attributes'] is None:
                self.cache['attributes'] = {k:v for k,v in attrs.items()}
            else:
                self.cache['attributes'].update(attrs)
            self.dirty.add('attributes')
        if self.live:
            try:
                dog.WorkspaceManager.update_attributes(self.workspace, attrs)
            except AssertionError:
                self.go_offline()
                self.pending.append((
                    'attributes',
                    partial(dog.WorkspaceManager.update_attributes, self.workspace, attrs),
                    partial(dog.WorkspaceManager.get_attributes, self.workspace)
                ))
        else:
            self.pending.append((
                'attributes',
                partial(dog.WorkspaceManager.update_attributes, self.workspace, attrs),
                partial(dog.WorkspaceManager.get_attributes, self.workspace)
            ))
        if self.live:
            try:
                self.cache['attributes'] = dog.WorkspaceManager.get_attributes(self.workspace)
            except AssertionError:
                pass

    def evaluate_expression(self, etype, entity, expression):
        if self.live:
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
                this.attributes
            )
        return evaluator(etype, entity, expression)
