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
from agutil.parallel import parallelize
from itertools import repeat
from glob import glob
import yaml
from ..cache import cached, cache_fetch, cache_write, cache_init
from ..adapters import NoSuchSubmission
import re

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
        ws = lapdog.WorkspaceManager(namespace, name)
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
def version():
    with open(os.path.join(os.path.dirname(os.path.dirname(__file__)), '__init__.py')) as reader:
        result = re.search(r'__version__ = \"(\d+\.\d+\.\d+.*?)\"', reader.read())
        if result:
            return result.group(1)
        return "Unknown"

@cached(120, 10)
def get_adapter(namespace, workspace, submission):
    return get_workspace_object(namespace, workspace).get_adapter(submission)

@cached(60)
def _get_lines(namespace, workspace, submission):
    adapter = get_adapter(namespace, workspace, submission)
    reader = adapter.read_cromwell()
    return reader, []

def get_lines(namespace, workspace, submission):
    # from ..adapters import do_select
    # reader, lines = _get_lines(namespace, workspace, submission)
    # print("READER", reader)
    # while len(do_select(reader, 1)[0]):
    #     lines.append(reader.readline().decode().strip())
    #     print("READLINE", lines[-1])
    # return lines
    reader, lines = _get_lines(namespace, workspace, submission)
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
def status():
    obj = {
        'health': requests.get('https://api.firecloud.org/health').text,
        'systems': {}
    }
    try:
        for key, val in requests.get('https://api.firecloud.org/status').json()['systems'].items():
            obj['systems'][key] = 'ok' in val and val['ok']
        for key, val in requests.get('https://rawls.dsde-prod.broadinstitute.org/status').json()['systems'].items():
            obj['systems'][key] = 'ok' in val and val['ok']
        obj['failed'] = False
    except:
        obj['failed'] = True
        print(traceback.format_exc())
    return obj, 200

@cached(120)
def list_workspaces():
    try:
        return sorted([
            {
                'accessLevel': workspace['accessLevel'],
                'owners': workspace['owners'],
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
                workspace = get_workspace_object(ns, ws).operator.firecloud_workspace
                workspace_data.append({
                    'accessLevel': workspace['accessLevel'],
                    'owners': workspace['owners'],
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
def workspace(namespace, name):
    ws = get_workspace_object(namespace, name)
    data = ws.operator.firecloud_workspace
    data['entities'] = [
        {**v, **{'type':k}}
        for k,v in ws.operator.entity_types.items()
    ]
    data['configs'] = get_configs(namespace, name)
    data['attributes'] = ws.attributes
    return data, 200

@cached(600)
def service_account():
    try:
        buff = io.StringIO(subprocess.check_output(
            'gcloud iam service-accounts list --format=json',
            shell=True,
            stderr=subprocess.PIPE
        ).decode())
    except:
        traceback.print_exc()
        return {
            'msg': 'Unable to read active service account',
            'error': traceback.format_exc()
        }, 500
    try:
        for acct in json.load(buff):
            if 'displayName' in acct and acct['displayName'] == 'Compute Engine default service account':
                return acct['email'], 200
        buff.seek(0,0)
        return {
            'msg': 'Unable to locate service account',
            'error': buff.read()
        }, 500
    except:
        traceback.print_exc()
        buff.seek(0,0)
        return {
            'msg': 'Unable to parse gcloud output',
            'error': buff.read()
        }, 500

@cached(60)
def get_acl(namespace, name):
    failure = 'NONE'
    result = fc.get_workspace_acl(namespace, name)
    if result.status_code == 403:
        return {
            'failed': True,
            'reason': 'permissions'
        }, 200
    if result.status_code != 200:
        print(result.text)
        return {
            'failed': True,
            'reason': 'firecloud',
            'error': result.text
        },result.status_code
    return {
        'accounts': [
            {
                'email': k,
                'access': v['accessLevel'],
                'share': v['canShare'],
                'compute': v['canCompute']
            }
            for k,v in result.json()['acl'].items()
        ],
        'failed': False,
        'reason': 'success'
    }, 200

@cached(60)
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

def set_acl(namespace, name):
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
        response = fc.update_workspace_acl(
            namespace,
            name,
            [{
                'email': account,
                'accessLevel': 'WRITER',
            }]
        )
        try:
            data = response.json()
        except:
            traceback.print_exc()
            return {
                'failed': True,
                'reason': 'firecloud'
            }, 200
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
    return ws.operator.get_entities_df(etype)

@cached(10)
def get_entities(namespace, name, etype, start=0, end=None):
    # result = get_workspace_object(namespace, name).operator.entity_types
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

def get_cache(namespace, name):
    ws = get_workspace_object(namespace, name)
    if ws.operator._webcache_:
        if ws.live and not (len(ws.operator.dirty) or len(ws.operator.pending)):
            return 'up-to-date'
        return 'outdated'
    return 'not-loaded'

def sync_cache(namespace, name):
    ws = get_workspace_object(namespace, name)
    ws.populate_cache()
    ws.operator._webcache_ = True
    return get_cache(namespace, name)

def create_workspace(namespace, name, parent):
    ws = lapdog.WorkspaceManager(namespace, name)
    parent = None if '/' not in parent else lapdog.WorkspaceManager(parent)
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
def get_configs(namespace, name):
    ws = get_workspace_object(namespace, name)
    return ws.list_configs()

@cached(20)
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

def preflight(namespace, name, config, entity, expression="", etype=""):
    ws = get_workspace_object(namespace, name)
    try:
        result = ws.execute_preflight(
            config,
            entity,
            expression if expression != "" else None,
            etype if etype != "" else None
        )
        if result[0]:
            _okay, _config, _entity, _etype, _workflow_entities, _template, _invalid_inputs = result
            return {
                'failed': False,
                'ok': _okay,
                'message': 'Okay',
                'workflows': len(_workflow_entities),
                'invalid_inputs': ', '.join(
                    value for value in _invalid_inputs
                )
            }, 200
        return {
            'failed': False,
            'ok': False,
            'message': result[1],
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

def execute(namespace, name, config, entity, expression="", etype="", memory=3, batch=250, query=100):
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
            query_limit=query
        )
        return {
            'failed': False,
            'ok': True,
            'global_id': global_id,
            'local_id': local_id,
            'operation_id': operation_id
        }, 200
    except:
        print(traceback.format_exc())
        return {
            'failed': True,
            'ok': False,
            'message': "Failure: "+repr(sys.exc_info()),
        }, 200

def decode_submission(submission_id):
    if submission_id.startswith('lapdog/'):
        ns, ws, sid = base64.b64decode(submission_id[7:].encode()).decode().split('/')
        return {
            'namespace': ns,
            'workspace': ws,
            'id': sid
        }, 200
    return "Not a valid submission id", 404


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

def get_cost(namespace, name, id):
    ws = get_workspace_object(namespace, name)
    try:
        adapter = get_adapter(namespace, name, id)
    except NoSuchSubmission:
        traceback.print_exc()
        return "No Such Submission", 404
    return adapter.cost()

def abort_submission(namespace, name, id):
    adapter = get_adapter(namespace, name, id)
    adapter.abort()
    return [wf.long_id for wf in adapter.workflows.values() if wf.long_id is not None], 200

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
def get_workflow(namespace, name, id, workflow_id):
    adapter = get_adapter(namespace, name, id)
    adapter.update()
    # print(adapter.workflows)
    if workflow_id[:8] in adapter.workflows:
        # Return data from workflow
        wf = adapter.workflows[workflow_id[:8]]
        entity_map = {w['workflowOutputKey']:w['workflowEntity'] for w in adapter.raw_workflows}
        reverse_map = {v:entity_map[k] for k,v in adapter.workflow_mapping.items()}
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
                    'idx': i
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
            )
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
            'entity': None
        }, 200

# @cached(30)
def read_logs(namespace, name, id, workflow_id, log, call):
    filename, suffix = {
        'stdout': ('stdout', '-stdout.log'),
        'stderr': ('stderr', '-stderr.log'),
        'google': (None, '.log')
    }[log]
    log_text = cache_fetch('workflow', id, workflow_id, dtype=str(call)+'.', ext=log+'.log')
    adapter = get_adapter(namespace, name, id)
    if log_text is not None:
        return log_text, 200
    adapter.update()
    from ..adapters import safe_getblob
    if workflow_id[:8] in adapter.workflows:
        workflow = adapter.workflows[workflow_id[:8]]
        if call < len(workflow.calls):
            call = workflow.calls[call]
            blob = None
            if filename is not None:
                path = os.path.join(
                    call.path,
                    filename
                )
                print("Trying", path)
                try:
                    blob = safe_getblob(path)
                except FileNotFoundError:
                    pass
            if blob is None:
                path = os.path.join(
                    call.path,
                    call.task + suffix
                )
                print("Trying", path)
                try:
                    blob = safe_getblob(path)
                except FileNotFoundError:
                    return "Not found", 404
            text = blob.download_as_string().decode()
            if not adapter.live:
                cache_write(text, 'workflow', id, workflow_id, dtype=str(call.attempt)+'.', ext=log+'.log')
            return text, 200
    return 'Error', 500

@cached(10)
def operation_status(operation_id):
    from ..adapters import get_operation_status
    # print("Getting status:", operation_id)
    text =  get_operation_status(operation_id, False, 'yaml')
    # print(text[:256])
    return text, 200

def cache_size():
    total = 0
    for path in glob(cache_init()+'/*'):
        total += os.path.getsize(path)
    return byteSize(total), 200

@cached(120)
def quotas(region):
    try:
        text = subprocess.run(
            'gcloud compute project-info describe',
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        ).stdout.decode()
        data = yaml.load(io.StringIO(text))['quotas']
    except:
        print(text)
        raise
    try:
        text = subprocess.run(
            'gcloud compute regions describe %s' % region,
            shell=True,
            executable='/bin/bash',
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        ).stdout.decode()
        data += [
            {
                **r,
                **{
                    'metric': region+'.'+r['metric']
                }
            }
            for r in yaml.load(io.StringIO(text))['quotas']
        ]
    except:
        print(text)
        raise
    alerts = [
        {
            **r,
            **{
                'percent': '%0.2f%%' % (100 * r['usage'] / r['limit'])
            }
        }
        for r in data if r['limit'] > 0 and r['usage']/r['limit'] > .5
    ]
    return {
        'alerts': alerts,
        'raw': data
    }, 200

@cached(60)
def get_config(namespace, name, config_namespace, config_name):
    ws = get_workspace_object(namespace, name)
    try:
         config = ws.operator.get_config_detail(config_namespace, config_name)
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
        return {
            'config':config,
            'io': io_types,
            'wdl': ws.operator.get_wdl(
                config['methodRepoMethod']['methodNamespace'],
                config['methodRepoMethod']['methodName'],
                config['methodRepoMethod']['methodVersion']
            )
        }, 200
    except:
        traceback.print_exc()
        return {
            'config':config,
            'wdl': None
        }, 200
    return "FAILED", 500

def update_config(namespace, name, config):
    ws = get_workspace_object(namespace, name)
    ws.update_configuration(config)
    get_config.cache_clear()
    get_configs.cache_clear()
    return ("OK", 200)

def delete_config(namespace, name, config_namespace, config_name):
    ws = get_workspace_object(namespace, name)
    ws.delete_config(config_namespace, config_name)
    get_config.cache_clear()
    get_configs.cache_clear()
    return ("OK", 200)

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
        result = get_workspace_object(namespace, name).update_configuration(
            config,
            method_filepath, # Either it's a file-object or None
            delete_old=False # UI never deletes old configurations
        )
        get_config.cache_clear()
        get_configs.cache_clear()
        return {
            'failed': not result,
            'reason': 'Success' if result else 'API rejected update'
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
