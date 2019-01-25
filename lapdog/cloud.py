# This module defines utilities for the cloud function api

import subprocess
import requests
import os
import json
from urllib.parse import quote
from hashlib import md5, sha256
import time
from google.cloud import storage
import google.auth
import google.oauth2.service_account
import google.oauth2.credentials
from google.auth.transport.requests import AuthorizedSession
from google.cloud import kms_v1 as kms
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, utils
import traceback
import warnings
import base64
import sys
import math

# TODO: Update all endpoints to v1 for release
__API_VERSION__ = {
    'submit': 'beta',
    'abort': 'beta',
    'register': 'beta',
    'signature': 'beta',
    'query': 'beta',
    'quotas': 'beta',
    'existence': 'frozen'
}
# The api version will allow versioning of cloud functions
# The patching system will be as follows:
# End users who update their lapdog will encounter errors because the cloud endpoints do not exist
# End users contact administrators, asking them to update
# Administrators can run lapdog patch {namespace} (to be implemented)
# Lapdog patch will run code from lapdog.patch.__project_admin_apply_patch
# This function will deploy new cloud functions and run any other arbitrary code
# Such as updating iam policy bindings or role permissions

__CROMWELL_TAG__ = 'gateway'

def _deploy(function, endpoint, service_account=None, project=None):
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
            os.path.join(tempdir, 'main.py')
        )
        cmd = 'gcloud {project} beta functions deploy {endpoint}-{version} --entry-point {function} --runtime python37 --trigger-http --source {path} {service_account}'.format(
            endpoint=endpoint,
            version=__API_VERSION__[endpoint],
            function=function,
            path=tempdir,
            service_account='' if service_account is None else ('--service-account '+service_account),
            project='' if project is None else ('--project '+project)
        )
        print(cmd)
        subprocess.check_call(
            cmd,
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
    try:
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

        # 1.b) Verify the user has a pet account
        response = query_service_account(
            generate_default_session(scopes=['https://www.googleapis.com/auth/cloud-platform']),
            ld_acct_in_project(token_data['email'])
        )
        if response.status_code != 200:
            return (
                {
                    'error': 'User has not registered with this Lapdog Engine',
                    'message': response.text
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

        # 2.b) Verify that the bucket belongs to this project

        if 'namespace' not in data or 'workspace' not in data:
            return (
                {
                    'error': 'Bad Request',
                    'message': 'Missing required parameters "namespace" and "workspace"'
                },
                400
            )
        core_session = generate_core_session()

        result, message = authenticate_bucket(
            data['bucket'], data['namespace'], data['workspace'], session, core_session
        )
        if not result:
            return (
                {
                    'error': 'Cannot Validate Bucket Signature',
                    'message': message
                },
                400
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

        if 'memory' in data and data['memory'] > 3072:
            # if data['memory'] > 13312:
            #     mtype = 'custom-2-%d-ext' % data['memory']
            # else:
            #     mtype = 'custom-2-%d' % data['memory']
            mtype = 'custom-%d-%d' % (
                math.ceil(data['memory']/13312)*2,
                data['memory']
            )
        else:
            mtype = 'n1-standard-1'

        pipeline = {
            'pipeline': {
                'actions': [
                    {
                        'imageUri': 'gcr.io/broad-cga-aarong-gtex/wdl_runner:' + __CROMWELL_TAG__,
                        'commands': [
                            '/wdl_runner/wdl_runner.sh'
                        ],
                        'environment': {
                            'SIGNATURE_ENDPOINT': 'https://{region}-{project}.cloudfunctions.net/signature-{version}'.format(
                                region=os.environ.get('FUNCTION_REGION'),
                                project=os.environ.get("GCP_PROJECT"),
                                version=__API_VERSION__['signature']
                            ),
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
                                "https://www.googleapis.com/auth/genomics"
                            ]
                        },
                        'bootDiskSizeGb': 20
                    }
                },
            }
        }
        print(pipeline)
        response = generate_default_session(
            [
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/compute",
                "https://www.googleapis.com/auth/genomics"
            ]
        ).post(
            'https://genomics.googleapis.com/v2alpha1/pipelines:run',
            headers={
                'Content-Type': 'application/json'
            },
            json=pipeline
        )
        try:
            if response.status_code == 200:
                operation = response.json()['name']

                # 5) Sign the operation

                sign_object(
                    (data['submission_id'] + operation).encode(),
                    getblob(
                        'gs://{bucket}/lapdog-executions/{submission_id}/signature'.format(
                            bucket=data['bucket'],
                            submission_id=data['submission_id']
                        ),
                        credentials=session.credentials
                    ),
                    core_session.credentials
                )

                return operation, 200
        except:
            traceback.print_exc()
            return (
                {
                    'error': 'Unable to start submission',
                    'message': traceback.format_exc()
                },
                500
            )
        return (
            {
                'error': 'Unable to start submission',
                'message': 'Google rejected the pipeline request (%d) : %s' % (response.status_code, response.text)
            },
            400
        )
    except:
        traceback.print_exc()
        return (
            {
                'error': 'Unknown Error',
                'message': traceback.format_exc()
            },
            500
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

def generate_default_session(scopes=None):
    return AuthorizedSession(google.auth.default(scopes=scopes)[0])

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


def authenticate_bucket(bucket, namespace, workspace, session, core_session):
    if os.environ.get('GCP_PROJECT') != ld_project_for_namespace(namespace):
        return False, 'This project is not responsible for the provided namespace'
    workspace_blob = getblob(
        'gs://{bucket}/DO_NOT_DELETE_LAPDOG_WORKSPACE_SIGNATURE'.format(
            bucket=bucket
        ),
        credentials=session.credentials
    )
    if not workspace_blob.exists():
        # Must generate signature
        print("Generating new workspace signature for", workspace)
        try:
            response = session.get(
                'https://api.firecloud.org/api/workspaces/{namespace}/{workspace}'.format(
                    namespace=namespace,
                    workspace=workspace
                ),
                timeout=5 # Long timeout because this is a 1-time operation
            )
            if response.status_code == 200:
                fc_bucket = response.json()['workspace']['bucketName']
                if fc_bucket != bucket:
                    return False, 'The provided bucket does not belong to this workspace'
                sign_object(
                    ('{}/{}/{}'.format(namespace, workspace, bucket)).encode(),
                    workspace_blob,
                    core_session.credentials
                )
                return True, fc_bucket
        except requests.ReadTimeout:
            pass
        return False, 'The Firecloud API is currently offline or took too long to respond'
    return (
        verify_signature(workspace_blob, ('{}/{}/{}'.format(namespace, workspace, bucket)).encode()),
        'Workspace authentication token had an invalid signature'
    )

def ld_project_for_namespace(namespace):
    prefix = ('ld-'+namespace)[:23]
    suffix = md5(prefix.encode()).hexdigest().lower()
    return prefix + '-' + suffix[:6]

def sign_object(data, blob, credentials):
    blob.upload_from_string(
        kms.KeyManagementServiceClient(
            credentials=credentials
        ).asymmetric_sign(
            'projects/{ld_project}/locations/us/keyRings/lapdog/cryptoKeys/lapdog-sign/cryptoKeyVersions/1'.format(
                ld_project=os.environ.get('GCP_PROJECT')
            ),
            {'sha256': sha256(data).digest()}
        ).signature
    )

def proxy_group_for_user(account):
    info = account.split('@')
    return info[0]+'-'+info[1].split('.')[0]+'-lapdog'

def verify_signature(blob, data, _is_blob=True):
    try:
        serialization.load_pem_public_key(
            kms.KeyManagementServiceClient().get_public_key(
                'projects/%s/locations/us/keyRings/lapdog/cryptoKeys/lapdog-sign/cryptoKeyVersions/1' % os.environ.get('GCP_PROJECT')
            ).pem.encode('ascii'),
            default_backend()
        ).verify(
            blob.download_as_string() if _is_blob else blob,
            sha256(data).digest(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=32
            ),
            utils.Prehashed(hashes.SHA256())
        )
        return True
    except InvalidSignature:
        return False

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
    return True, 'OK'

def fetch_submission_blob(session, bucket, submission_id):
    return getblob(
        'gs://{bucket}/lapdog-executions/{submission_id}/submission.json'.format(
            bucket=bucket,
            submission_id=submission_id
        ),
        credentials=session.credentials
    )

def check_user_service_account(account, session=None):
    if session is None:
        session = generate_default_session([
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/iam'
        ])
    response = session.get(
        'https://iam.googleapis.com/v1/projects/{project}/serviceAccounts/{account}'.format(
            project=os.environ.get('GCP_PROJECT'),
            account=quote(account)
        )
    )
    if response.status_code == 200:
        return response.json()
    return None

def update_iam_policy(session, grants, project=None):
    if project is None:
        project = os.environ.get('GCP_PROJECT')
    policy = session.post(
        'https://cloudresourcemanager.googleapis.com/v1/projects/{project}:getIamPolicy'.format(
            project=project
        )
    ).json()
    for account_email, role in grants.items():
        target_role = 'projects/{}/roles/{}'.format(
            project,
            role
        )
        if target_role in {binding['role'] for binding in policy['bindings']}:
            for i, binding in enumerate(policy['bindings']):
                if binding['role'] == target_role:
                    policy['bindings'][i]['members'].append(
                        account_email
                    )
        else:
            policy['bindings'].append(
                {
                    'role': target_role,
                    'members': [
                        account_email
                    ]
                }
            )
    response = session.post(
        'https://cloudresourcemanager.googleapis.com/v1/projects/{project}:setIamPolicy'.format(
            project=project
        ),
        headers={'Content-Type': 'application/json'},
        json={
            "policy": policy,
            "updateMask": "bindings"
        }
    )
    return response.status_code == 200, response


@cors('DELETE')
def abort_submission(request):
    try:

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

        signature_blob = getblob(
            'gs://{bucket}/lapdog-executions/{submission_id}/signature'.format(
                bucket=data['bucket'],
                submission_id=data['submission_id']
            ),
            credentials=session.credentials
        )
        if not signature_blob.exists():
            return (
                {
                    'error': 'No Signature',
                    'message': 'The submission signature could not be found. Refusing to abort job'
                },
                403
            )

        if not verify_signature(signature_blob, (data['submission_id'] + submission['operation']).encode()):
            return (
                {
                    'error': 'Invalid Signature',
                    'message': 'Could not validate submission signature. Refusing to abort job'
                },
                403
            )

        core_session = generate_core_session()

        # 5) Generate abort key

        sign_object(
            data['submission_id'].encode(),
            getblob(
                'gs://{bucket}/lapdog-executions/{submission_id}/abort-key'.format(
                    bucket=data['bucket'],
                    submission_id=data['submission_id']
                ),
                credentials=session.credentials
            ),
            core_session.credentials
        )

        if 'hard' in data and data['hard']:
            # 6) Abort operation
            response = core_session.post(
                "https://genomics.googleapis.com/v2alpha1/{operation}:cancel".format(
                    operation=quote(submission['operation']) # Do not quote slashes here
                )
            )

            return response.text, response.status_code
        return (
            {
                'status': 'Aborting',
                'message': 'A soft-abort request has been sent.'
                ' If the submission does not abort soon, abort it with hard=True to force-kill the cromwell server'
            },
            200
        )
    except:
        traceback.print_exc()
        return (
            {
                'error': 'Unknown Error',
                'message': traceback.format_exc()
            },
            500
        )

@cors("POST")
def check_abort(request):
    data = request.get_json()
    if not ('key' in data and 'id' in data):
        return "missing params", 400
    if verify_signature(base64.b64decode(data['key'].encode()), data['id'].encode(), _is_blob=False):
        return 'OK', 200
    return 'ERROR', 400

@cors("POST")
def register(request):
    try:
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

        # 2.b) Verify that the bucket belongs to this project

        if 'namespace' not in data or 'workspace' not in data:
            return (
                {
                    'error': 'Bad Request',
                    'message': 'Missing required parameters "namespace" and "workspace"'
                },
                400
            )
        core_session = generate_core_session()

        result, message = authenticate_bucket(
            data['bucket'], data['namespace'], data['workspace'], session, core_session
        )
        if not result:
            return (
                {
                    'error': 'Cannot Validate Bucket Signature',
                    'message': message
                },
                400
            )

        # 3) Issue worker account

        default_session = generate_default_session(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        account_email = ld_acct_in_project(token_data['email'])
        response = query_service_account(default_session, account_email)
        if response.status_code == 404:
            account_name = account_email.split('@')[0]
            response = default_session.post(
                'https://iam.googleapis.com/v1/projects/{project}/serviceAccounts'.format(
                    project=os.environ.get('GCP_PROJECT')
                ),
                headers={'Content-Type': 'application/json'},
                json={
                    'accountId': account_name,
                    'serviceAccount': {
                        'displayName': token_data['email']
                    }
                }
            )
            if response.status_code >= 400:
                return (
                    {
                        'error': 'Unable to issue service account',
                        'message': response.text
                    },
                    400
                )
        elif response.status_code >= 400:
            return (
                {
                    'error': 'Unable to query service account',
                    'message': response.text
                },
                400
            )
        if response.json()['email'] != account_email:
            return (
                {
                    'error': 'Service account email did not match expected value',
                    'message': response.json()['email'] + ' != ' + account_email
                },
                400
            )

        # 4) Update worker bindings

        response = default_session.post(
            'https://iam.googleapis.com/v1/projects/{project}/serviceAccounts/{account}:setIamPolicy'.format(
                project=os.environ.get('GCP_PROJECT'),
                account=account_email
            ),
            headers={'Content-Type': 'application/json'},
            json={
                "policy": {
                    "bindings": [
                        {
                            "role": "roles/iam.serviceAccountUser",
                            "members": [
                                "serviceAccount:{email}".format(email=os.environ.get("FUNCTION_IDENTITY")),
                                "serviceAccount:{email}".format(email=account_email)
                            ]
                        }
                    ]
                },
                "updateMask": "bindings"
            }
        )

        if response.status_code != 200:
            return (
                {
                    'error': 'Unable to update service account bindings',
                    'message': '(%d) : %s' % (response.status_code, response.text)
                }
            )

        # 5) Update project bindings

        status, response = update_iam_policy(
            default_session,
            {
                'serviceAccount:'+account_email: 'Pet_account',
                'user:'+token_data['email']: 'Lapdog_user'
            }
        )

        if not status:
            return (
                {
                    'error': 'Unable to update project IAM policy',
                    'message': '(%d) : %s' % (response.status_code, response.text)
                },
                400
            )

        # 6) Generate Key

        response = default_session.post(
            'https://iam.googleapis.com/v1/projects/{project}/serviceAccounts/{email}/keys'.format(
                project=os.environ.get('GCP_PROJECT'),
                email=quote(account_email)
            )
        )
        if response.status_code >= 400:
            return (
                {
                    'error': 'Unable to issue service account key',
                    'message': response.text
                },
                400
            )

        # 7) Register with Firecloud

        time.sleep(10)

        pet_session = AuthorizedSession(
            google.oauth2.service_account.Credentials.from_service_account_info(
                json.loads(base64.b64decode(response.json()['privateKeyData']).decode())
            ).with_scopes([
                'https://www.googleapis.com/auth/userinfo.profile',
                'https://www.googleapis.com/auth/userinfo.email'
            ])
        )
        while True:
            try:
                response = pet_session.post(
                    "https://api.firecloud.org/register/profile",
                    headers={
                        'User-Agent': 'FISS/0.16.9',
                        'Content-Type': 'application/json'
                    },
                    json={
                        "firstName":"Service",
                        "lastName": "Account",
                        "title":"None",
                        "contactEmail":token_data['email'],
                        "institute":"None",
                        "institutionalProgram": "None",
                        "programLocationCity": "None",
                        "programLocationState": "None",
                        "programLocationCountry": "None",
                        "pi": "None",
                        "nonProfitStatus": "false"
                    },
                    timeout=10
                )
                break
            except google.auth.exceptions.RefreshError:
                time.sleep(10) # need more time for key to propagate
        if response.status_code != 200:
            return (
                {
                    'error': "Unable to register account with firecloud",
                    'message': response.text
                },
                400
            )

        # 8) Check ProxyGroup

        response = session.get('https://api.firecloud.org/api/groups', headers={'User-Agent': 'FISS/0.16.9'}, timeout=5)

        if response.status_code != 200:
            return (
                {
                    'error': "Unable to enumerate user's groups",
                    'message': response.text
                },
                400
            )

        target_group = proxy_group_for_user(token_data['email'])

        for group in response.json():
            if group['groupName'] == target_group:
                # 9) Register Account in Group
                response = session.put(
                    'https://api.firecloud.org/api/groups/{group}/member/{email}'.format(
                        group=target_group,
                        email=quote(account_email)
                    ),
                    timeout=5
                )
                if response.status_code != 204:
                    return (
                        {
                            'error': 'Unable to add pet account to proxy group',
                            'message': "Please manually add {email} to {group}".format(
                                group=target_group,
                                email=quote(account_email)
                            )
                        },
                        400
                    )
                else:
                    return (
                        account_email,
                        200
                    )

        # 8.b) Create Group
        response = sesion.post(
            'https://api.firecloud.org/api/groups/{group}'.format(
                group=target_group
            ),
            timeout=5
        )
        if response.status_code >= 400:
            return (
                {
                    'error': 'Unable to create Firecloud proxy group',
                    'message': response.text
                },
                400
            )
        # 9) Register Account in Group
        response = session.put(
            'https://api.firecloud.org/api/groups/{group}/member/{email}'.format(
                group=target_group,
                email=quote(account_email)
            ),
            timeout=5
        )
        if response.status_code != 204:
            return (
                {
                    'error': 'Unable to add pet account to proxy group',
                    'message': "Please manually add {email} to {group}".format(
                        group=target_group+'@firecloud.org',
                        email=quote(account_email)
                    )
                },
                400
            )
        else:
            return (
                account_email,
                200
            )
    except requests.ReadTimeout:
        return (
            {
                'error': 'timeout to firecloud',
                'message': 'Took longer than 5 seconds for Firecloud to respond. Please try again later'
            },
            400
        )
    except:
        traceback.print_exc()
        return (
            {
                'error': 'Unknown Error',
                'message': traceback.format_exc()
            },
            500
        )

def query_service_account(session, account):
    return session.get(
        'https://iam.googleapis.com/v1/projects/{project}/serviceAccounts/{account}'.format(
            project=os.environ.get('GCP_PROJECT'),
            account=quote(account)
        )
    )

@cors('POST')
def query_account(request):
    try:
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

        # 2) Check service account
        default_session = generate_default_session(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        account_email = ld_acct_in_project(token_data['email'])
        response = query_service_account(default_session, account_email)
        if response.status_code >= 400:
            return (
                {
                    'error': 'Unable to query service account',
                    'message': response.text
                },
                400
            )
        if response.json()['email'] != account_email:
            return (
                {
                    'error': 'Service account email did not match expected value',
                    'message': response.json()['email'] + ' != ' + account_email
                },
                400
            )
        return account_email, 200

    except:
        traceback.print_exc()
        return (
            {
                'error': 'Unknown Error',
                'message': traceback.format_exc()
            },
            500
        )

@cors('GET')
def existence(request):
    return 'OK', 200

@cors('POST')
def quotas(request):
    try:
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

        # 2) Check service account
        default_session = generate_default_session(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        account_email = ld_acct_in_project(token_data['email'])
        response = query_service_account(default_session, account_email)
        if response.status_code >= 400:
            return (
                {
                    'error': 'Unable to query service account',
                    'message': response.text
                },
                400
            )
        if response.json()['email'] != account_email:
            return (
                {
                    'error': 'Service account email did not match expected value',
                    'message': response.json()['email'] + ' != ' + account_email
                },
                400
            )

        # 3) Query quota usage
        project_usage = default_session.get(
            'https://www.googleapis.com/compute/v1/projects/{project}'.format(
                project=os.environ.get('GCP_PROJECT')
            )
        )
        if project_usage.status_code != 200:
            return (
                {
                    'error': 'Invalid response from Google',
                    'message': '(%d) : %s' % (
                        project_usage.status_code,
                        project_usage.text
                    )
                },
                400
            )
        region_usage = default_session.get(
            'https://www.googleapis.com/compute/v1/projects/{project}/regions/{region}'.format(
                project=os.environ.get('GCP_PROJECT'),
                region=os.environ.get('FUNCTION_REGION')
            )
        )
        if region_usage.status_code != 200:
            return (
                {
                    'error': 'Invalid response from Google',
                    'message': '(%d) : %s' % (
                        region_usage.status_code,
                        region_usage.text
                    )
                },
                400
            )
        region_name = os.environ.get('FUNCTION_REGION')
        quotas = [
            {
                **quota,
                **{
                    'percent':  ('%0.2f%%' % (100 * quota['usage'] / quota['limit'])) if quota['limit'] > 0 else '0.00%'
                }
            }
            for quota in project_usage.json()['quotas']
        ] + [
            {
                **quota,
                **{
                    'percent':  ('%0.2f%%' % (100 * quota['usage'] / quota['limit'])) if quota['limit'] > 0 else '0.00%',
                    'metric': region_name+'.'+quota['metric']
                }
            }
            for quota in region_usage.json()['quotas']
        ]
        return (
            {
                'raw': quotas,
                'alerts': [quota for quota in quotas if quota['limit'] > 0 and quota['usage']/quota['limit'] >= 0.5]
            },
            200
        )
    except:
        traceback.print_exc()
        return (
            {
                'error': 'Unknown Error',
                'message': traceback.format_exc()
            },
            500
        )

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser('cloud-deployment')
    parser.add_argument(
        'function',
        help='Name of function in code'
    )
    parser.add_argument(
        'endpoint',
        help='Name of desired endpoint'
    )
    args = parser.parse_args()
    _deploy(args.function, args.endpoint)
