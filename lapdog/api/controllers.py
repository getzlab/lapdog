import requests
import sys
from firecloud import api as fc
import subprocess
import io
import time
from functools import lru_cache
from flask import current_app
import random
import pandas as pd
import lapdog
import os
import json
import base64
import traceback
import select
from agutil import byteSize
from agutil.parallel import parallelize, parallelize2
from itertools import repeat
from glob import glob
import yaml
from .. import firecloud_status
from ..cache import cached, cache_fetch, cache_write, cache_init, cache_path
from ..adapters import NoSuchSubmission, Gateway, get_operation_status
from ..gateway import get_account, proxy_group_for_user
import re
import contextlib
import pickle
import tempfile
import html
from .. import getblob
from urllib.parse import unquote
from google.api_core.exceptions import Forbidden, BadRequest


@parallelize2(1)
def get_womtool():
    data = cache_fetch('static', 'womtool', '39', 'jar', decode=False)
    if data is None:
        print("Downloading womtool for syntax highlighting")
        data = requests.get('https://github.com/broadinstitute/cromwell/releases/download/39/womtool-39.jar').content
        cache_write(data, 'static', 'womtool', '39', 'jar', decode=False)
    return cache_path('static')('womtool', '39', 'jar', dtype='data', ext='')

@contextlib.contextmanager
def log_controller(func):
    try:
        start = time.time()
        print("Started serving", func.__name__)
        yield
    finally:
        print("Finished serving", func.__name__, "in %0.1f seconds" % (time.time() - start))

def controller(func):
    def wrapper(*args, **kwargs):
        with log_controller(func):
            return func(*args, **kwargs)
    return wrapper

def readvar(obj, *args):
    current = obj
    for arg in args:
        if arg not in current:
            return None
        current = current[arg]
    return current

def get_workspace_object(namespace, name):
    ws = readvar(current_app.config, 'storage', 'cache', namespace, name, 'manager')
    if ws is None:
        ws = lapdog.WorkspaceManager(namespace, name, workspace_seed_url=None)
        if readvar(current_app.config, 'storage', 'cache') is None:
            current_app.config['storage']['cache'] = {}
        if readvar(current_app.config, 'storage', 'cache', namespace) is None:
            current_app.config['storage']['cache'][namespace] = {}
        if readvar(current_app.config, 'storage', 'cache', namespace, name) is None:
            current_app.config['storage']['cache'][namespace][name] = {}
        if readvar(current_app.config, 'storage', 'cache', namespace, name, 'manager') is None:
            current_app.config['storage']['cache'][namespace][name]['manager'] = ws
        workspaces = readvar(current_app.config, 'storage', 'cache', 'all_workspaces')
        if workspaces is None:
            current_app.config['storage']['cache']['all_workspaces'] = []
        current_app.config['storage']['cache']['all_workspaces'].append((namespace, name))
    return ws

@lru_cache()
@controller
def version():
    with open(os.path.join(os.path.dirname(os.path.dirname(__file__)), '__init__.py')) as reader:
        result = re.search(r'__version__ = \"(\d+\.\d+\.\d+.*?)\"', reader.read())
        if result:
            return result.group(1)
        return "Unknown"

@cached(120, 10)
def get_adapter(namespace, workspace, submission):
    return get_workspace_object(namespace, workspace).get_adapter(submission)

# @cached(60)
# def _get_lines(namespace, workspace, submission):
#     adapter = get_adapter(namespace, workspace, submission)
#     reader = adapter.read_cromwell()
#     return reader, []

def get_lines(namespace, workspace, submission):
    # from ..adapters import do_select
    # reader, lines = _get_lines(namespace, workspace, submission)
    # print("READER", reader)
    # while len(do_select(reader, 1)[0]):
    #     lines.append(reader.readline().decode().strip())
    #     print("READLINE", lines[-1])
    # return lines
    reader = get_adapter(namespace, workspace, submission).read_cromwell()
    lines = []
    from select import select
    try:
        if len(select([reader], [], [], 10)[0]):
            lines.append(reader.readline().decode().strip())
        while len(select([reader], [], [], 1)[0]):
            lines.append(reader.readline().decode().strip())
    except:
        lines.append([line.decode().strip() for line in reader.readlines()])
    return lines

@cached(30)
@controller
def status():
    try:
        return (
            {
                **{'failed': False},
                **firecloud_status()
            },
            200
        )
    except:
        print(traceback.format_exc())
    return {'failed': True, 'systems': {}}, 200

@cached(120)
@controller
def list_workspaces():
    try:
        return sorted([
            {
                'accessLevel': workspace['accessLevel'],
                'public': workspace['public'],
                'namespace': workspace['workspace']['namespace'],
                'name': workspace['workspace']['name'],
                'bucket': workspace['workspace']['bucketName'],
                'id': workspace['workspace']['workspaceId']
            } for workspace in fc.list_workspaces().json()
        ], key=lambda x:(x['namespace'], x['name'])), 200
    except:
        print(traceback.format_exc())
        all_workspaces = readvar(current_app.config, 'storage', 'cache', 'all_workspaces')
        if all_workspaces is not None:
            workspace_data = []
            for ns, ws, in all_workspaces:
                workspace = get_workspace_object(ns, ws).firecloud_workspace
                workspace_data.append({
                    'accessLevel': workspace['accessLevel'],
                    'public': False,
                    'namespace': ns,
                    'name': ws,
                    'bucket': workspace['workspace']['bucketName'],
                    'id': workspace['workspace']['workspaceId']
                })
            return sorted(workspace_data, key=lambda x:(x['namespace'], x['name'])), 200
    return {
        'failed': True,
        'reason': "The Firecloud api is currently offline, and there are no workspaces in the cache"
    }, 500

@cached(120)
@controller
def workspace(namespace, name):
    ws = get_workspace_object(namespace, name)
    data = ws.firecloud_workspace
    data['entities'] = [
        {**v, **{'type':k}}
        for k,v in ws.entity_types.items()
    ]
    data['configs'] = get_configs(namespace, name)
    data['attributes'] = ws.attributes
    return data, 200


@cached(600)
@controller
def get_namespace_registered(namespace, name):
    ws = get_workspace_object(namespace, name)
    exists = ws.gateway.exists
    return {
        'exists': exists,
        'registered': exists and ws.gateway.registered,
        'compute_regions': ws.gateway.compute_regions if ws.gateway.exists else []
    }

@controller
def register(namespace, name):
    ws = get_workspace_object(namespace, name)
    if ws.gateway is not None and ws.gateway.exists and not ws.gateway.registered:
        ws.gateway.register(ws.workspace, ws.bucket_id)
        get_namespace_registered.cache_clear()
    return get_namespace_registered(namespace, name)



@cached(120)
@controller
def service_account():
    return proxy_group_for_user(get_account())+'@firecloud.org', 200

@cached(60)
@controller
def get_acl(namespace, name):
    failure = 'NONE'
    ws = get_workspace_object(namespace, name)
    try:
        acl = ws.acl
    except ValueError:
        traceback.print_exc()
        return (
            {
                'failed': True,
                'reason': 'permissions'
            },
            200
        )
    except NameError as e:
        traceback.print_exc()
        return (
            {
                'failed': True,
                'reason': 'firecloud',
                'error': e._request_text if hasattr(e, '_request_text') else "UNKNOWN"
            },
            200
        )
    except ValueError as e:
        traceback.print_exc()
        return (
            {
                'failed': True,
                'reason': 'firecloud',
                'error': e._request_text if hasattr(e, '_request_text') else "UNKNOWN"
            },
            200
        )
    return ({
        'accounts': [
            {
                'email': k,
                'access': v['accessLevel'],
                'share': v['canShare'],
                'compute': v['canCompute']
            }
            for k,v in acl.items()
        ],
        'failed': False,
        'reason': 'success'
    }, 200)

@cached(60)
@controller
def service_acl(namespace, name):
    data, code = get_acl(namespace, name)
    if code != 200 or (data['failed'] and data['reason'] != 'permissions'):
        return {
            'failed': True,
            'reason': data['reason']
        }, code
    perms = data['reason'] == 'permissions'
    account, code = service_account()
    if code != 200:
        print(account)
        return {
            'failed': True,
            'reason': 'gcloud',
        }, 200
    response = getattr(fc, '__get')('/api/proxyGroup/%s'%account)
    if response.status_code == 404:
        return {
            'failed': True,
            'reason': 'registration'
        }, 200
    elif response.status_code != 200:
        print(response.text)
        return {
            'failed': True,
            'reason': 'firecloud'
        }, 200
    ws, code = workspace(namespace, name)
    if code != 200:
        print(ws)
        return {
            'failed': True,
            'reason': 'acl-read'
        }, 200

    account_data = [acct for acct in data['accounts'] if acct['email'] == account] if 'accounts' in data else []

    return {
        'failed': perms,
        'reason': 'success' if not perms else 'permissions',
        'share': ws['canShare'],
        'service_account': len(account_data) and len({acct['access'] for acct in account_data} & {'WRITER', 'OWNER', 'ADMIN', 'PROJECT_OWNER'})
    }, 200

@controller
def set_acl(namespace, name):
    ws = get_workspace_object(namespace, name)
    account, code = service_account()
    if code != 200:
        print(account)
        return {
            'failed': True,
            'reason': 'gcloud',
        }, 200
    response = getattr(fc, '__get')('/api/proxyGroup/%s'%account)
    if response.status_code == 404:
        return {
            'failed': True,
            'reason': 'registration'
        }, 200
    elif response.status_code != 200:
        print(response.text)
        return {
            'failed': True,
            'reason': 'firecloud'
        }, 200
    acl, code = service_acl(namespace, name)
    if code != 200 or acl['failed']:
        print(acl, code)
        return {
            'failed': True,
            'reason': 'acl-read',
        }, 200
    if not acl['service_account']:
        if not acl['share']:
            return {
                'failed': True,
                'reason': 'permissions'
            }, 200
        try:
            data = ws.update_acl({
                account: 'WRITER'
            })
        except Exception as e:
            traceback.print_exc()
            return {
                'failed': True,
                'reason': 'firecloud',
                'error': e._request_text if hasattr(e, '_request_text') else 'UNKNOWN'
            }
        if 'usersUpdated' in data or 'invitesSent' in data or 'usersNotFound' in data:
            if 'usersNotFound' in data and account in {acct['email'] for acct in data['usersNotFound']}:
                return {
                    'failed': True,
                    'reason': 'registration'
                }, 200
            elif 'usersUpdated' not in data or account not in {acct['email'] for acct in data['usersUpdated']}:
                print(data)
                return {
                    'failed': True,
                    'reason': 'firecloud'
                }, 200
        else:
            print(data)
            return {
                'failed': True,
                'reason': 'firecloud'
            }, 200
    return {
        'failed': False,
        'reason': 'success',
        'share': True,
        'service_account': True
    }, 200

@cached(60)
def _get_entitites_df(namespace, name, etype):
    ws = get_workspace_object(namespace, name)
    return ws._get_entities_internal(etype)

@cached(10)
@controller
def get_entities(namespace, name, etype, start=0, end=None):
    # result = get_workspace_object(namespace, name).entity_types
    # return {
    #     'failed': False,
    #     'reason': 'success',
    #     'entity_types': sorted([
    #         {
    #             'type': key,
    #             'attributes': val['attributeNames'],
    #             'n': val['count'],
    #             'id': val['idName'],
    #             # 'cache': get_cache(namespace, name, key)
    #         } for key, val in result.items()
    #     ], key=lambda x:x['type'])
    # }, 200
    df = _get_entitites_df(namespace, name, etype).reset_index()
    buffer = io.StringIO()
    if end is not None:
        df.iloc[start:end].to_json(buffer, 'records')
    else:
        df.to_json(buffer, 'records')
    return json.loads(buffer.getvalue())

@controller
def get_cache(namespace, name):
    ws = get_workspace_object(namespace, name)
    return {'state': ws.live, 'pending': len(ws.pending_operations)}, 200

@controller
def sync_cache(namespace, name):
    ws = get_workspace_object(namespace, name)
    if ws.live:
        ws.populate_cache()
        ws._webcache_ = True
        ws.go_offline()
    else:
        ws.sync()
    return get_cache(namespace, name)

@controller
def create_workspace(namespace, name, parent):
    ws = lapdog.WorkspaceManager(namespace, name, workspace_seed_url=None)
    parent = None if '/' not in parent else lapdog.WorkspaceManager(parent, workspace_seed_url=None)
    with lapdog.capture() as (stdout, stderr):
        result = ws.create_workspace(parent)
        stdout.seek(0,0)
        stderr.seek(0,0)
        text = stdout.read() + stderr.read()
    try:
        text = json.loads(text.strip())['message']
    except:
        pass
    if not result:
        return {
            'failed': True,
            'reason': text
        }, 200
    return {
        'failed': False,
        'reason': 'success'
    }

@cached(60)
@controller
def get_configs(namespace, name):
    ws = get_workspace_object(namespace, name)
    get_womtool() # enque a download without waiting
    return ws.list_configs()

@cached(20)
@controller
def list_submissions(namespace, name, cache):
    from ..lapdog import timestamp_format
    ws = get_workspace_object(namespace, name)
    return sorted(
        (
            sub for sub in ws.list_submissions(lapdog_only=True, cached=cache)
            if 'identifier' in sub
        ),
        key=lambda s:(
            time.strptime(s['submissionDate'], timestamp_format)
            if s['submissionDate'] != 'TIME'
            else time.gmtime()
            ),
        reverse=True
    )

@controller
def preflight(namespace, name, config, entity, expression="", etype=""):
    ws = get_workspace_object(namespace, name)
    try:
        result = ws.preflight(
            config,
            entity,
            expression if expression != "" else None,
            etype if etype != "" else None
        )
        if result.result:
            return {
                'failed': False,
                'ok': result.result,
                'message': 'Okay',
                'workflows': len(result.workflow_entities),
                'invalid_inputs': ', '.join(
                    value for value in result.invalid_inputs
                )
            }, 200
        return {
            'failed': False,
            'ok': False,
            'message': result.reason,
            'workflows': 0,
            'invalid_inputs': 'None'
        }, 200
    except:
        print(sys.exc_info())
        return {
            'failed': True,
            'ok': False,
            'message': "Failure: "+traceback.format_exc(),
            'workflows': 0,
            'invalid_inputs': 'None'
        }, 200

@controller
def execute(namespace, name, config, entity, expression="", etype="", memory=3, batch=250, query=100, private=False, region=None):
    ws = get_workspace_object(namespace, name)
    try:
        global_id, local_id, operation_id = ws.execute(
            config,
            entity,
            expression if expression != "" else None,
            etype if etype != "" else None,
            force=True,
            memory=memory,
            batch_limit=batch,
            query_limit=query,
            private=private,
            region=region
        )
        return {
            'failed': False,
            'ok': True,
            'global_id': global_id,
            'local_id': local_id,
            'operation_id': operation_id
        }, 200
    except:
        traceback.print_exc()
        return {
            'failed': True,
            'ok': False,
            'message': traceback.format_exc(),
        }, 200

@controller
def decode_submission(submission_id):
    if submission_id.startswith('lapdog/'):
        ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
        return {
            'namespace': ns,
            'workspace': ws,
            'id': sid
        }, 200
    return "Not a valid submission id", 404

@controller
def get_submission(namespace, name, id):
    ws = get_workspace_object(namespace, name)
    try:
        adapter = get_adapter(namespace, name, id)
    except NoSuchSubmission:
        traceback.print_exc()
        return "No Such Submission", 404
    return {
        **ws.get_submission(id),
        **{
            'gs_path': os.path.join(
                'gs://'+ws.get_bucket_id(),
                'lapdog-executions',
                id
            ),
        }
    }, 200

@controller
def get_cost(namespace, name, id):
    ws = get_workspace_object(namespace, name)
    try:
        adapter = get_adapter(namespace, name, id)
    except NoSuchSubmission:
        traceback.print_exc()
        return "No Such Submission", 404
    return adapter.cost()

@controller
def abort_submission(namespace, name, id):
    adapter = get_adapter(namespace, name, id)
    adapter.abort()
    return [wf.long_id for wf in adapter.workflows.values() if wf.long_id is not None], 200

@controller
def upload_submission(namespace, name, id):
    ws = get_workspace_object(namespace, name)
    try:
        stats = ws.complete_execution(id)
    except FileNotFoundError:
        traceback.print_exc()
        return {
            'failed': True,
            'message': 'Job did not complete. It may have been aborted'
        }, 200
    except:
        traceback.print_exc()
        return {
            'failed': True,
            'message': 'Exception: '+ traceback.format_exc()
        }, 500
    else:
        return {
            'failed': not stats,
            'message': 'Failed'
        }, 200

@controller
def read_cromwell(namespace, name, id, offset=0):
    # _get_lines.cache_clear()
    lines = get_lines(namespace, name, id)
    return lines[offset:], 200
    # adapter = get_adapter(namespace, name, id)
    # reader = adapter.read_cromwell()
    # from ..adapters import do_select
    # lines = []
    # while len(do_select(reader, 1)[0]):
    #     lines.append(reader.readline().decode().strip())
    # return lines

@cached(10)
@controller
def get_workflows(namespace, name, id):
    adapter = get_adapter(namespace, name, id)
    adapter.update()
    return [
        {
            'entity': wf['workflowEntity'],
            'id': adapter.workflow_mapping[wf['workflowOutputKey']],
            'status': (
                adapter.workflows[adapter.workflow_mapping[wf['workflowOutputKey']][:8]].status
                if adapter.workflow_mapping[wf['workflowOutputKey']][:8] in adapter.workflows
                else 'Starting'
            )
        }
        for wf in adapter.raw_workflows
        if wf['workflowOutputKey'] in adapter.workflow_mapping
    ], 200

# @cached(10)
@cached(10)
@controller
def get_workflow(namespace, name, id, workflow_id):
    adapter = get_adapter(namespace, name, id)
    adapter.update(5)
    # print(adapter.workflows)
    if workflow_id[:8] in adapter.workflows:
        # Return data from workflow
        wf = adapter.workflows[workflow_id[:8]]
        entity_map = {w['workflowOutputKey']:w['workflowEntity'] for w in adapter.raw_workflows}
        reverse_map = {v:entity_map[k] for k,v in adapter.workflow_mapping.items()}
        workflow_inputs = None
        try:
            workflow_inputs = wf.inputs
        except:
            traceback.print_exc()
        return {
            'id': wf.long_id,
            'short_id': wf.id,
            'key': wf.key,
            'running': wf.started,
            'n_calls': len(wf.calls),
            'calls': [
                {
                    'status': call.status,
                    'message': call.last_message,
                    'attempt': call.attempt,
                    'operation': call.operation,
                    'task': call.task,
                    'gs_path': call.path,
                    'code': call.return_code,
                    'idx': i,
                    'runtime': call.runtime
                }
                for i, call in enumerate(wf.calls)
            ],
            'gs_path': wf.path,
            'status': wf.status,
            'failure': wf.failure,
            'entity': (
                reverse_map[workflow_id]
                if workflow_id in reverse_map
                else None
            ),
            'inputs': workflow_inputs
        }, 200
    else:
        return {
            'id': None,
            'short_id': None,
            'key': None,
            'running': False,
            'n_calls': 0,
            'calls': [],
            'gs_path': None,
            'status': 'Pending',
            'failure': None,
            'entity': None,
            'inputs': None
        }, 200

# @cached(30)
@controller
def read_logs(namespace, name, id, workflow_id, log, call):
    log_text = cache_fetch('workflow', id, workflow_id, dtype=str(call)+'.', ext=log+'.log')
    if log_text is not None:
        return log_text, 200
    adapter = get_adapter(namespace, name, id)
    adapter.update()
    if workflow_id[:8] in adapter.workflows:
        workflow = adapter.workflows[workflow_id[:8]]
        if call < len(workflow.calls):
            call = workflow.calls[call]
            try:
                text = call.read_log(log)
                return text, 200
            except FileNotFoundError:
                return "Not Found", 404
    return "Error", 500

@cached(10)
@controller
def operation_status(operation_id):
    return get_operation_status(operation_id, False, 'yaml'), 200

@controller
def cache_size():
    total = 0
    for path, _, files in os.walk(cache_init()):
        for f in files:
            total += os.path.getsize(os.path.join(path, f))
    return byteSize(total), 200

@cached(120)
@controller
def quotas(namespace):
    try:
        return Gateway(namespace).quota_usage
    except NameError:
        traceback.print_exc()
        print("No resolution found")
        return {
            'raw': [],
            'alerts': []
        }


@cached(60)
@controller
def get_config(namespace, name, config_namespace, config_name):
    ws = get_workspace_object(namespace, name)
    try:
         config = ws.get_config(config_namespace, config_name)
    except:
        ws.sync()
        return (
            {
                'config': None,
                'wdl': None
            }, 200)
    try:
        data = getattr(fc, '__post')(
            '/api/inputsOutputs',
            data=json.dumps({
                'methodNamespace': config['methodRepoMethod']['methodNamespace'],
                'methodName': config['methodRepoMethod']['methodName'],
                'methodVersion': config['methodRepoMethod']['methodVersion']
            })
        ).json()
        io_types = {
            'inputs': {
                param['name']:{
                    'type': param['inputType'],
                    'required': not param['optional']
                }
                for param in data['inputs']
            },
            'outputs': {
                param['name']:param['outputType']
                for param in data['outputs']
            }
        }
    except KeyError:
        traceback.print_exc()
        print("The above error is likely caused by a missing WDL", file=sys.stderr)
        io_types = None
    except:
        traceback.print_exc()
        io_types = None
    try:
        wdl_text = ws.get_wdl(
            config['methodRepoMethod']['methodNamespace'],
            config['methodRepoMethod']['methodName'],
            config['methodRepoMethod']['methodVersion']
        )
        try:
            with tempfile.NamedTemporaryFile('w', suffix='wdl') as tmpwdl:
                tmpwdl.write(wdl_text)
                tmpwdl.flush()
                result = subprocess.run(
                    'java -jar {} highlight {} --highlight-mode html'.format(get_womtool()(), tmpwdl.name),
                    shell=True,
                    executable='/bin/bash',
                    stdout=subprocess.PIPE
                )
                if result.returncode == 0:
                    raw_text = result.stdout.decode()
                    wdl_text = ''
                    pattern = re.compile(r'</?span.*?>')
                    while pattern.search(raw_text):
                        match = pattern.search(raw_text)
                        wdl_text += (
                            html.escape(raw_text[:match.start()])
                            + match.group()
                        )
                        raw_text = raw_text[match.end():]
                    wdl_text += html.escape(raw_text)
        except:
            traceback.print_exc()
        return {
            'config':config,
            'io': io_types,
            'wdl': wdl_text
        }, 200
    except:
        traceback.print_exc()
        return {
            'config':config,
            'wdl': None
        }, 200
    return "FAILED", 500

@controller
def update_config(namespace, name, config):
    ws = get_workspace_object(namespace, name)
    ws.update_configuration(config)
    get_config.cache_clear()
    get_configs.cache_clear()
    return ("OK", 200)

@controller
def delete_config(namespace, name, config_namespace, config_name):
    ws = get_workspace_object(namespace, name)
    ws.delete_config(config_namespace, config_name)
    get_config.cache_clear()
    get_configs.cache_clear()
    return ("OK", 200)

@controller
def upload_config(namespace, name, config_filepath, method_filepath=None):
    try:
        config = json.load(config_filepath)
    except:
        traceback.print_exc()
        return {
            'failed': True,
            'reason': "Unable to parse config json"
        }, 200
    try:
        ws = get_workspace_object(namespace, name)
        live = ws.live
        result = ws.update_configuration(
            config,
            method_filepath, # Either it's a file-object or None
        )
        get_config.cache_clear()
        get_configs.cache_clear()
        return {
            'failed': not result,
            'reason': 'Success' if result else (
                'API rejected update' if live else 'Workspace Offline. WDL cannot be synced to FireCloud'
            )
        }
    except KeyError:
        traceback.print_exc()
        return {
            'failed': True,
            'reason': "Configuration missing required keys"
        }, 200
    except:
        traceback.print_exc()
        return {
            'failed': True,
            'reason': "Unhandled exception. Check Lapdog terminal output for details"
        }, 200

@controller
def rerun_submission(namespace, name, id):
    ws = get_workspace_object(namespace, name)
    return {
        **{
            'name': None,
            'type': None,
            'expression': None
        },
        **ws.build_retry_set(id)
    }

@cached(120)
@controller
def config_autocomplete(namespace, name, config_namespace, config_name):
    ws = get_workspace_object(namespace, name)
    entityTypeMetadata = ws.entity_types[ws.get_config(config_namespace, config_name)['rootEntityType']]
    return (
        [
            'workspace.'+attribute for attribute in ws.attributes
        ] + [
        'this.'+attribute for attribute in entityTypeMetadata['attributeNames']
        ] + ['this.name', 'this.'+entityTypeMetadata['idName']]
    ), 200

@controller
def autocomplete(namespace, name, entity):
    """
    Entrypoint for autocomplete system
    Uses heavy caching to accelerate results
    1) Checks for previous autocomplete searches in this workspace which could serve as a starting point
    2) For any searches which are a substring of the current search, fetch those results and then filter using the current search
    3) Otherwise, start from scratch (fetching new entities again) and filter using the current search
    """
    if len(entity) <= 1:
        return []
    for search in [*get_autocomplete_cache(namespace, name)]:
        if search in entity:
            return [
                eid for eid in get_autocomplete_entries(namespace, name, search)
                if entity in eid
            ]
    return get_autocomplete_entries(namespace, name, entity)

@cached(60)
def get_autocomplete_cache(namespace, name):
    return set()

@cached(60, cache_size=64)
def get_autocomplete_entries(namespace, name, entity):
    ws = get_workspace_object(namespace, name)
    get_autocomplete_cache(namespace, name).add(entity)
    return [
        eid
        for etype in ws.entity_types
        for eid in ws._get_entities_internal(etype).index
        if entity in eid
    ]

@controller
def seed_cache(namespace, name):
    ws = get_workspace_object(namespace, name)
    return {
        key:base64.b64encode(pickle.dumps(value)).decode()
        for key, value in ws.cache.items()
    }

__USER_PROJECT = None

def get_user_project(project=None):
    global __USER_PROJECT
    if project is not None:
        __USER_PROJECT = project
    else:
        if __USER_PROJECT is None:
            proj = subprocess.run(
                'gcloud config get-value billing/quota_project',
                shell=True,
                stdout=subprocess.PIPE
            ).stdout.decode().strip()
            if proj == 'CURRENT_PROJECT' or proj == '':
                proj = subprocess.run(
                    'gcloud config get-value project',
                    shell=True,
                    stdout=subprocess.PIPE
                ).stdout.decode().strip()
                __USER_PROJECT = (
                    proj if proj != '' else None
                )

    return __USER_PROJECT


@cached(15)
@controller
def preview_blob(path, project=None):
    return _preview_blob_internal(
        unquote(path.replace('~', '%')),
        project
    )

def _preview_blob_internal(path, project):
    if not path.startswith('gs://'):
        path = 'gs://'+path
    blob = getblob(path, user_project=project)
    try:
        exists = blob.exists()
        blob.bucket.reload()
        if exists: # file
            blob.reload()
            return (
                {
                    'requesterPays': blob.bucket.requester_pays,
                    'exists': exists,
                    'size': byteSize(blob.size),
                    'url': None, # Temporary. Need a way to generate signed urls
                    'preview': blob.download_as_string(end=1024).decode('ascii', 'replace'),
                    'visitUrl': "https://console.cloud.google.com/storage/browser/{}/{}".format(
                        blob.bucket.name,
                        os.path.dirname(blob.name)
                    ),
                    'children': [],
                    'bucket': blob.bucket.name
                },
                200
            )
        elif len([*blob.bucket.list_blobs(prefix=blob.name, max_results=1)]):
            # is a directory
            return (
                {
                    'requesterPays': blob.bucket.requester_pays,
                    'exists': False,
                    'size': None,
                    'preview': None,
                    'url': None,
                    'visitUrl': "https://console.cloud.google.com/storage/browser/{}/{}".format(
                        blob.bucket.name,
                        blob.name
                    ),
                    'children': sorted({
                        os.path.relpath(child.name, blob.name).split('/')[0]
                        for page in blob.bucket.list_blobs(prefix=blob.name).pages
                        for child in page
                    }),
                    'bucket': blob.bucket.name
                }
            )
    except BadRequest as e:
        if 'requester pays' in e.message.lower():
            project = get_user_project(project)
            if project is not None:
                return _preview_blob_internal(path, project)
            return (
                {
                    'requesterPays': True,
                    'exists': False,
                    'size': None,
                    'preview': None,
                    'url': None,
                    'visitUrl': "https://console.cloud.google.com/storage/browser/{}/{}".format(
                        blob.bucket.name,
                        os.path.dirname(blob.name)
                    ),
                    'children': [],
                    'bucket': blob.bucket.name
                },
                200
            )
        traceback.print_exc()
        return (
            {
                "error": "Unable to query path",
                "message": traceback.format_exc()
            },
            400
        )
    except Forbidden:
        traceback.print_exc()
    except:
        traceback.print_exc()
        return (
            {
                "error": "Unable to query path",
                "message": traceback.format_exc()
            },
            500
        )
    return (
        {
            'requesterPays': False,
            'exists': False,
            'size': None,
            'preview': None,
            'url': None,
            'visitUrl': "https://console.cloud.google.com/storage/browser/{}/{}".format(
                blob.bucket.name,
                os.path.dirname(blob.name)
            ),
            'children': [],
            'bucket': blob.bucket.name
        },
        200
    )
