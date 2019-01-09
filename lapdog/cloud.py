# This module defines utilities for the cloud function api

import subprocess
import requests
import os
import json
from urllib.parse import quote
from hashlib import md5

def cloud_api_endpoint(request):
    if request.method == 'POST':
        data = request.get_json()
        # Global request format
        # {
        #     'auth': "<Issuer's auth token>",
        #     'method': "<Lapdog API method name>",
        #     'args': ['...'],
        #     'kwargs': {'...'}
        # }

def cors(*methods):
    """
    1) OPTIONS requests are returned with the accepted list of methods and localhost only origins
    2) Issues 405 responses to requests with bad methods
    3) Issues 403 responses to requests with bad origins
    4) Passes on requests to the decorated function only if they have an accepted method and localhost origin
    """
    def wrapper(func):
        def call(request):
            if request.method == 'OPTIONS':
                headers = {
                    'Access-Control-Allow-Origin': (
                        'http://localhost:4201' if not (
                            'Origin' in request.headers and request.headers['Origin'].startswith('http://localhost')
                        ) else request.headers['Origin']
                    ),
                    'Access-Control-Allow-Methods': ', '.join(methods),
                }
                return ('', 204, headers)
            elif request.method not in methods:
                return ('Not allowed', 405, {'Allow': ', '.join(methods)})
            elif 'Origin' in request.headers and not request.headers['Origin'].startswith('http://localhost'):
                return ('Forbidden', 403)
            return func(request)
        return call
    return wrapper

@cors('POST')
def create_submission(request):
    data = request.get_json()

    # 1) Validate the token

    if 'token' not in data:
        return (
            {
                'error': 'Bad Request',
                'message': 'Missing required parameter "token"'
            },
            400
        )

    token_data = get_token_info(data['token'])
    if 'error' in token_data:
        return (
            {
                'error': 'Invalid Token',
                'message': token_data['error_description'] if 'error_description' in token_data else 'Google rejected the client token'
            },
            401
        )

    # 2) Validate user's permission for the bucket

    if 'bucket' not in data:
        return (
            {
                'error': 'Bad Request',
                'message': 'Missing required parameter "bucket"'
            },
            400
        )

    read, write = validate_permissions(data['token'], data['bucket'])
    if read is None:
        # Error, write will contain a message
        return (
            {
                'error': 'Cannot Validate Bucket Permissions',
                'message': write
            },
            400
        )
    if not (read and write):
        # User doesn't have full permissions to the bucket
        return (
            {
                'error': 'Not Authorized',
                'message': 'User lacks read/write permissions to the requested bucket'
            },
            401
        )

    # 3) Check that submission.json exists, and is less than 1 Gib

    if 'submission_id' not in data:
        return (
            {
                'error': 'Bad Request',
                'message': 'Missing required parameter "submission_id"'
            },
            400
        )

    result, message = validate_submission_file(data['token'], data['bucket'], data['submission_id'])
    if not result:
        return (
            {
                'error': 'Bad Submission',
                'message': message
            },
            400
        )

    # 4) Submit pipelines request

    pipeline = {
        'pipeline': {
            'actions': [
                {
                    'imageUri': 'gcr.io/broad-cga-aarong-gtex/wdl_runner:v0.9.0',
                    'commands': [
                        '/wdl_runner/wdl_runner.sh'
                    ],
                    'environment': {
                        'WDL': "gs://{bucket}/lapdog-executions/{submission_id}/method.wdl".format(
                            bucket=data['bucket'],
                            submission_id=data['submission_id']
                        ),
                        'WORKFLOW_INPUTS': "gs://{bucket}/lapdog-executions/{submission_id}/config.tsv".format(
                            bucket=data['bucket'],
                            submission_id=data['submission_id']
                        ),
                        'WORKFLOW_OPTIONS': json.dumps(data['options']) if 'options' in data else '{}',
                        'LAPDOG_SUBMISSION_ID': data['submission_id'],
                        'WORKSPACE': "gs://{bucket}/lapdog-executions/{submission_id}/workspace/".format(
                            bucket=data['bucket'],
                            submission_id=data['submission_id']
                        ),
                        'OUTPUTS': "gs://{bucket}/lapdog-executions/{submission_id}/results".format(
                            bucket=data['bucket'],
                            submission_id=data['submission_id']
                        ),
                        'SUBMISSION_DATA_PATH': "gs://{bucket}/lapdog-executions/{submission_id}/submission.json".format(
                            bucket=data['bucket'],
                            submission_id=data['submission_id']
                        ),
                        'LAPDOG_LOG_PATH': "gs://{bucket}/lapdog-executions/{submission_id}/logs/".format(
                            bucket=data['bucket'],
                            submission_id=data['submission_id']
                        ),
                    }
                }
            ],
            'resources': {
                'projectId': os.environ.get("GCP_PROJECT"),
                'regions': ['us-central1'], # FIXME
                'virtualMachine': {
                    'machineType': 'n1-standard-1' or 'custom-2-{memory}' or 'custom-2-{memory}-ext', # FIXME
                    'preemptible': False,
                    'labels': {
                        'lapdog-execution-role': 'cromwell',
                        'lapdog-submission-id': data['submission_id']
                    },
                    'serviceAccount': {
                        'email': ld_acct_in_project(token_data['email']),
                        'scopes': [
                            "https://www.googleapis.com/auth/cloud-platform",
                            "https://www.googleapis.com/auth/compute",
                            "https://www.googleapis.com/auth/devstorage.read_write",
                        ]
                    },
                    'bootDiskSizeGb': 20
                }
            },
        }
    }
    response = requests.post(
        'https://content-genomics.googleapis.com/v2alpha1/pipelines:run?alt=json',
        headers={
            'Authorization': 'Bearer ' + token
        },
        data=pipeline
    )
    try:
        if response.status_code == 200:
            return response.json()['name'], 200
    except:
        pass
    return (
        {
            'error': 'Unable to start submission',
            'message': 'Google rejected the pipeline request (%d) : %s' % (response.status_code, response.text)
        },
        400
    )

def get_token_info(token):
    try:
        data = requests.get('https://www.googleapis.com/oauth2/v1/tokeninfo?access_token='+token).json()
        return data
    except:
        return None

def ld_acct_in_project(account, ld_project=None):
    if ld_project is None:
        ld_project = os.environ.get('GCP_PROJECT')
    return 'lapdog-'+ md5(account.encode()).hexdigest() + '@' + ld_project + '.iam.gserviceaccount.com'

def validate_permissions(token, bucket):
    try:
        response = requests.get(
            "https://www.googleapis.com/storage/v1/b/{bucket}"
            "/iam/testPermissions?permissions=storage.objects.list&"
            "permissions=storage.objects.get&permissions=storage.objects.create&"
            "permissions=storage.objects.delete".format(bucket=quote(bucket, safe='')),
            headers={
                'Authorization': 'Bearer {token}'.format(token=token)
            }
        )
        if response.status_code == 200:
            data = response.json()
            if 'permissions' in data:
                return (
                    'storage.objects.list' in data['permissions'] or 'storage.objects.get' in data['permissions'],
                    'storage.objects.create' in data['permissions'] and 'storage.objects.delete' in data['permissions']
                )
            else:
                return False, False
        elif response.status_code == 401:
            data = response.json()
            if 'error' in data and 'message' in data['error']:
                if data['error']['message'] == 'Invalid Credentials':
                    return None, 'Expired Credentials'
                elif data['error']['message'] == 'Not Found':
                    return None, 'Bucket Not Found'
        return None, 'Invalid Credentials'
    except:
        return None, 'Unexpected Error'

def validate_submission_file(token, bucket, submission_id):
    try:
        response = requests.get(
            "https://www.googleapis.com/storage/v1/b/{bucket}/o/{submission_file}".format(
                bucket=quote(bucket, safe=''),
                submission_file=quote(
                    os.path.join('lapdog-executions', submission_id, 'submission.json'),
                    safe=''
                )
            ),
            headers={
                'Authorization': 'Bearer {token}'.format(token=token)
            }
        )
        # 1073741824
        if response.status_code == 200:
            data = response.json()
            if int(data['size']) <= 1073741824: # 1Gib
                return True, 'Success'
            else:
                return False, 'Submission.json file exceeded maximum allowed size'
        elif response.status_code == 404:
            return False, 'Submission.json file not found'
        return False, 'Unable to query submission file (%d) : %s' % (response.status_code, response.text)
    except:
        return False, 'Unexpected Error'
