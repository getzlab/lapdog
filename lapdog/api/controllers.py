import requests
import sys
from firecloud import api as fc

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
