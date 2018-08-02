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
import json
import base64

def readvar(obj, *args):
    current = obj
    for arg in args:
        if arg not in current:
            return None
        current = current[arg]
    return current


def cached(timeout):

    def wrapper(func):

        def cacheapply(*args, **kwargs):
            print("Cache expired. Running", func)
            return (time.time(), func(*args, **kwargs))

        cachefunc = lru_cache(4)(cacheapply)

        def call_func(*args, **kwargs):
            result = cachefunc(*args, **kwargs)
            if time.time() - result[0] > timeout:
                cachefunc.cache_clear()
                result = cachefunc(*args, **kwargs)
            else:
                print("Cache intact. Retrieving cached results from", func)
            return result[1]

        call_func.cache_clear = cachefunc.cache_clear

        return call_func

    return wrapper

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
    return ws

@cached(30)
def status():
    obj = {
        'health': requests.get('https://api.firecloud.org/health').text,
        'systems': {}
    }
    try:
        for key, val in requests.get('https://api.firecloud.org/status').json()['systems'].items():
            obj['systems'][key] = bool(val['ok'])
        obj['failed'] = False
    except:
        obj['failed'] = True
        print(sys.exc_info())
    return obj, 200

@cached(120)
def list_workspaces():
    # print("REQUESTING", fc.list_workspaces())
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

@cached(120)
def workspace(namespace, name):
    response = fc.get_workspace(namespace, name)
    ws = get_workspace_object(namespace, name)
    data = response.json()
    data['entities'] = [
        {**v, **{'type':k}}
        for k,v in ws.operator.entity_types.items()
    ]
    data['configs'] = get_configs(namespace, name)
    return data, response.status_code

@cached(60)
def service_account():
    try:
        buff = io.StringIO(subprocess.check_output(
            'gcloud iam service-accounts list',
            shell=True,
            stderr=subprocess.PIPE
        ).decode())
    except:
        return {
            'msg': 'Unable to read active service account',
            'error': repr(sys.exc_info())
        }, 500
    try:
        return pd.read_fwf(buff).loc[0]['EMAIL'], 200
    except:
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
    ws, code = workspace(namespace, name)
    if code != 200:
        print(ws)
        return {
            'failed': True,
            'reason': 'acl-read'
        }, 200

    account_data = [acct for acct in data['accounts'] if acct['email'] == account] if 'accounts' in data else []

    return {
        'failed': False,
        'reason': 'success' if not perms else 'permissions',
        'share': ws['canShare'],
        'service_account': len(account_data) and len({acct['access'] for acct in account_data} & {'READER', 'WRITER', 'OWNER', 'ADMIN', 'PROJECT_OWNER'})
    }

def set_acl(namespace, name):
    account, code = service_account()
    if code != 200:
        print(account)
        return {
            'failed': True,
            'reason': 'gcloud',
        }, 200
    acl, code = service_acl(namespace, name)
    if code != 200 or acl['failed']:
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
                'canShare': False,
                'canCompute': False
            }]
        )
        try:
            data = response.json()
        except:
            return {
                'failed': True,
                'reason': 'firecloud'
            }, 200
        if 'usersUpdated' in data or 'invitesSent' in data or 'usersNotFound' in data:
            if 'usersNotFound' in data and account in {acct['email'] for acct in data['usersNotFound']}:
                return {
                    'failed': True,
                    'reason': 'account'
                }, 200
            elif 'usersUpdated' not in data or account not in {acct['email'] for acct in data['usersUpdated']}:
                return {
                    'failed': True,
                    'reason': 'firecloud'
                }, 200
        else:
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

@cached(10)
def get_entities(namespace, name):
    result = fc.__get('/api/workspaces/{}/{}/entities'.format(namespace, name))
    if result.status_code >= 400:
        return {
            'failed': True,
            'reason': result.txt
        }, result.status_code
    return {
        'failed': False,
        'reason': 'success',
        'entity_types': sorted([
            {
                'type': key,
                'attributes': val['attributeNames'],
                'n': val['count'],
                'id': val['idName'],
                # 'cache': get_cache(namespace, name, key)
            } for key, val in result.json().items()
        ], key=lambda x:x['type'])
    }, 200

def get_cache(namespace, name):
    ws = get_workspace_object(namespace, name)
    if ws.operator._webcache_:
        if ws.live and not (len(ws.operator.dirty) or len(ws.operator.pending)):
            return 'up-to-date'
        return 'outdated'
    return 'not-loaded'

def sync_cache(namespace, name):
    ws = get_workspace_object(namespace, name)
    was_live = ws.live
    ws.get_attributes()
    for etype in ws.operator.entity_types:
        ws.operator.get_entities_df(etype)
    for config, data in ws.list_configs().items():
        ws.operator.get_config_detail(data['namespace'], data['name'])
        try:
            ws.operator.get_wdl(
                data['methodRepoMethod']['methodNamespace'],
                data['methodRepoMethod']['methodName'],
                data['methodRepoMethod']['methodVersion']
            )
        except NameError:
            # WDL Doesnt exist
            pass
    if was_live:
        ws.sync()
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
        print("Failed to decode json:", text)
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
    return [*ws.list_configs().values()]

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
            'message': "Failure: "+repr(sys.exc_info()),
            'workflows': 0,
            'invalid_inputs': 'None'
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
    return ws.get_submission(id), 200
