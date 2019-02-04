from google.cloud import storage
from google.cloud import kms_v1 as kms
import google.auth
from google.auth.transport.requests import AuthorizedSession
import google.oauth2.service_account
import google.oauth2.credentials
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import ec, padding, utils
import requests
from hashlib import md5, sha256
import os
import json
from urllib.parse import quote
import time

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

def ld_project_for_namespace(namespace):
    prefix = ('ld-'+namespace)[:23]
    suffix = md5(prefix.encode()).hexdigest().lower()
    return prefix + '-' + suffix[:6]

def proxy_group_for_user(account):
    info = account.split('@')
    return info[0]+'-'+info[1].split('.')[0]+'-lapdog'

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
    response = query_service_account(session, account)
    if response.status_code == 200:
        return response.json()
    return None

def query_service_account(session, account):
    return session.get(
        'https://iam.googleapis.com/v1/projects/{project}/serviceAccounts/{account}'.format(
            project=os.environ.get('GCP_PROJECT'),
            account=quote(account)
        )
    )

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
