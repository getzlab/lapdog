# This module defines utilities for the cloud function api

import subprocess
import requests
import os
import json
from urllib.parse import quote
from hashlib import md5, sha256
import time
from google.cloud import storage
import google.oauth2.service_account
import google.oauth2.credentials
from google.auth.transport.requests import AuthorizedSession
from google.cloud import kms_v1 as kms

def _deploy(function, endpoint):
    import tempfile
    import shutil
    with tempfile.TemporaryDirectory() as tempdir:
        with open(os.path.join(tempdir, 'requirements.txt'), 'w') as w:
            w.write('google-auth\n')
            w.write('google-cloud-storage\n')
            w.write('google-cloud-kms\n')
            w.write('cryptography\n')
        shutil.copyfile(
            __file__,
            os.path.join(tempdir, 'cloud.py')
        )
        subprocess.check_call(
            'gcloud functions deploy {endpoint} --entry-point {function} --runtime python37 --trigger-http --source {path}'.format(
                endpoint=endpoint,
                function=function,
                path=tempdir
            ),
            shell=True
        )

def getblob(gs_path, credentials=None, user_project=None):
    bucket_id = gs_path[5:].split('/')[0]
    bucket_path = '/'.join(gs_path[5:].split('/')[1:])
    return storage.Blob(
        bucket_path,
        storage.Client(credentials=credentials).bucket(bucket_id, user_project)
    )

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
            result = list(func(request))
            print("RESULT", result)
            if isinstance(result[0], dict):
                result[0] = json.dumps(result[0])
            return tuple(result)
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

    session = generate_user_session(data['token'])

    read, write = validate_permissions(session, data['bucket'])
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

    submission = fetch_submission_blob(session, data['bucket'], data['submission_id'])

    result, message = validate_submission_file(submission)
    if not result:
        return (
            {
                'error': 'Bad Submission',
                'message': message
            },
            400
        )

    # 4) Submit pipelines request

    if 'memory' in data:
        if data['memory'] > 13312:
            mtype = 'custom-2-%d-ext' % data['memory']
        else:
            mtype = 'custom-2-%d' % data['memory']
    else:
        mtype = 'n1-standard-1'

    pipeline = {
        'pipeline': {
            'actions': [
                {
                    'imageUri': 'gcr.io/broad-cga-aarong-gtex/wdl_runner:gateway',
                    'commands': [
                        '/wdl_runner/wdl_runner.sh'
                    ],
                    'environment': {
                        'LAPDOG_PROJECT': os.environ.get('GCP_PROJECT'),
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
                        'LAPDOG_LOG_PATH': "gs://{bucket}/lapdog-executions/{submission_id}/logs".format(
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
                    'machineType': mtype,
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
    print(pipeline)
    response = session.post(
        'https://genomics.googleapis.com/v2alpha1/pipelines:run',
        headers={
            'Content-Type': 'application/json'
        },
        json=pipeline
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
    return ('lapdog-'+ md5(account.encode()).hexdigest())[:30] + '@' + ld_project + '.iam.gserviceaccount.com'

def ld_meta_bucket_for_project(ld_project=None):
    if ld_project is None:
        ld_project = os.environ.get('GCP_PROJECT')
    return 'ld-metadata-'+md5(ld_project.encode()).hexdigest()

def generate_user_session(token):
    return AuthorizedSession(
        google.oauth2.credentials.Credentials(token)
    )

def generate_core_session():
    ld_project = os.environ.get('GCP_PROJECT')
    account = 'lapdog-worker@{}.iam.gserviceaccount.com'.format(ld_project)
    t = int(time.time())
    key_data = json.loads(getblob(
        'gs://{bucket}/auth_key.json'.format(
            bucket=ld_meta_bucket_for_project(ld_project)
        )
    ).download_as_string().decode())
    return AuthorizedSession(
        google.oauth2.service_account.Credentials.from_service_account_info(key_data).with_scopes([
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/genomics',
            'https://www.googleapis.com/auth/devstorage.read_write'
        ])
    )


def validate_permissions(session, bucket):
    try:
        response = session.get(
            "https://www.googleapis.com/storage/v1/b/{bucket}"
            "/iam/testPermissions?permissions=storage.objects.list&"
            "permissions=storage.objects.get&permissions=storage.objects.create&"
            "permissions=storage.objects.delete".format(bucket=quote(bucket, safe='')),
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

def validate_submission_file(blob):
    if not blob.exists():
        return False, 'Submission.json file not found'
    blob.reload()
    if blob.size is None or blob.size >= 1073741824: # 1Gib
        return False, 'Submission.json file exceeded maximum allowed size'
    return True

def fetch_submission_blob(session, bucket, submission_id):
    return getblob(
        'gs://{bucket}/lapdog-executions/{submission_id}/submission.json'.format(
            bucket=bucket,
            submission_id=submission_id
        ),
        credentials=session.credentials
    )

def fetch_operation_submission_path(token, operation):
    try:
        response = requests.get(
            "https://genomics.googleapis.com/v2alpha1/{name=projects/*/operations/*}"
        )
    except:
        pass


@cors('DELETE')
def abort_submission(request):

    # https://genomics.googleapis.com/google.genomics.v2alpha1.Pipelines

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

    session = generate_user_session(data['token'])

    read, write = validate_permissions(session, data['bucket'])
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

    submission = fetch_submission_blob(session, data['bucket'], data['submission_id'])

    result, message = validate_submission_file(submission)
    if not result:
        return (
            {
                'error': 'Bad Submission',
                'message': message
            },
            400
        )

    # 4) Download submission and parse operation

    try:
        submission = json.loads(submission.download_as_string().decode())
    except:
        return (
            {
                'error': 'Invalid Submission',
                'message': 'Submission was not valid JSON'
            },
            400
        )

    if 'operation' not in submission:
        return (
            {
                'error': 'Invalid Submission',
                'message': 'Submission contained no operation metadata'
            },
            400
        )

    core_session = generate_core_session()

    response = core_session.post(
        "https://genomics.googleapis.com/v2alpha1/{operation}:cancel".format(
            operation=quote(submission['operation']) # Do not quote slashes here
        )
    )

    # 5) Generate abort key

    getblob(
        'gs://{bucket}/lapdog-executions/{submission_id}/abort-key'.format(
            bucket=data['bucket'],
            submission_id=data['submission_id']
        ),
        credentials=session.credentials
    ).upload_from_string(
        kms.KeyManagementServiceClient(
            credentials=core_session.credentials
        ).asymmetric_sign(
            'projects/{ld_project}/locations/us/keyRings/lapdog/cryptoKeys/lapdog-sign/cryptoKeyVersions/1'.format(
                ld_project=os.environ.get('GCP_PROJECT')
            ),
            {'sha256': sha256(data['submission_id'].encode()).digest()}
        ).signature
    )

    return response.text, response.status_code
