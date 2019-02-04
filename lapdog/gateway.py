# Submission Gateway
# Use as a modular component for adapter to interact with submission
# If submission JSON does not contain version field or version is 1, gcloud compute instances ssh
# If submission JSON contains version and version is 2, ssh -i {lapdog ssh token} {instance ip}
# If SSH fails for any reason, save the exception and try reading the log
# If the log is not found, then you're SOL

from functools import lru_cache
import requests
import subprocess
from hashlib import md5
from .cache import cached, cache_fetch, cache_write
import time
import warnings
import crayons
import os
import json
from .cloud.utils import get_token_info, ld_project_for_namespace, ld_meta_bucket_for_project, getblob, proxy_group_for_user, generate_user_session, update_iam_policy, __API_VERSION__
from urllib.parse import quote
import sys
import tempfile
import base64
from agutil import cmd as run_cmd

id_rsa = os.path.join(
    os.path.expanduser('~'),
    '.ssh',
    'id_rsa'
)

credentials_file = os.path.join(
    os.path.expanduser('~'),
    '.config',
    'gcloud',
    'application_default_credentials.json'
)

@cached(60, 1)
def get_account():
    return subprocess.run(
        'gcloud config get-value account',
        shell=True,
        stdout=subprocess.PIPE
    ).stdout.decode().strip()

@cached(10)
def get_access_token(account=None):
    if account is None:
        account = get_account()
    token = cache_fetch('token', 'access-token', md5(account.encode()).hexdigest(), google='credentials')
    if token and get_token_expired(token):
        token = None
    if token is None:
        if not os.path.isfile(credentials_file):
            raise FileNotFoundError("Application Default Credentials not found. Please run `gcloud auth application-default login`")
        with open(credentials_file) as r:
            credentials = json.load(r)
        response = requests.post(
            'https://www.googleapis.com/oauth2/v4/token',
            data={
                'client_id': credentials['client_id'],
                'client_secret': credentials['client_secret'],
                'refresh_token': credentials['refresh_token'],
                'grant_type': 'refresh_token'
            }
        )
        if response.status_code == 200:
            data = response.json()
            token = data['access_token']
            expiry = int(time.time() + int(data['expires_in']))
            cache_write(token, 'token', 'access-token', md5(account.encode()).hexdigest(), google='credentials')
            cache_write(str(expiry), 'token', 'expiry', token=md5(token.encode()).hexdigest())
        else:
            raise ValueError("Unable to refresh access token (%d) : %s" % (response.status_code, response.text))
    return token

def get_token_expired(token):
    expiry = cache_fetch('token', 'expiry', token=md5(token.encode()).hexdigest())
    if expiry is None:
        try:
            data = get_token_info(token)
            if data is None:
                return True
            expiry = int(time.time() + int(data['expires_in']))
            cache_write(str(expiry), 'token', 'expiry', token=md5(token.encode()).hexdigest())
        except:
            return True
    return int(expiry) < time.time()

def generate_core_key(ld_project, session=None):
    if session is None:
        session = generate_user_session(get_access_token())
    warnings.warn("Generating new root authentication key for project")
    response = session.post(
        "https://iam.googleapis.com/v1/projects/{project}/serviceAccounts/{account}/keys".format(
            project=quote(ld_project, safe=''),
            account=quote('lapdog-worker@{}.iam.gserviceaccount.com'.format(ld_project), safe='')
        )
    )
    if response.status_code == 200:
        getblob(
            'gs://{bucket}/auth_key.json'.format(
                bucket=ld_meta_bucket_for_project(ld_project)
            )
        ).upload_from_string(
            base64.b64decode(response.json()['privateKeyData'].encode())
        )
    else:
        print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
        raise ValueError("Could not generate core key")


class Gateway(object):
    """Acts as an interface between local lapdog and any resources behind the project's API"""

    def __init__(self, namespace):
        self.namespace = namespace
        self.project = ld_project_for_namespace(namespace)
        if not self.exists:
            warnings.warn("Gateway does not exist")
        elif not self.registered:
            warnings.warn(
                "Gateway for namespace {} not registered. Please run Gateway.register()".format(
                    self.namespace
                )
            )

    @classmethod
    def initialize_lapdog_for_project(cls, billing_id, project_id):
        """
        Initializes the lapdog execution API on the given firecloud project.
        Charges for operating the Lapdog API and for executing jobs will be billed
        to the provided billing id.
        The current gcloud account (when this function is executed) will be the Owner
        of the service and the only user capable of using it.
        Call authorize_user to allow another user to use the service
        """
        cmd = (
            'gcloud projects create {project_id}'.format(
                project_id=ld_project_for_namespace(project_id)
            )
        )
        print("Creating project")
        print(cmd)
        result = run_cmd(cmd)
        if result.returncode != 0 and b'already in use' not in result.buffer:
            raise ValueError("Unable to create project")
        print("Enabling servies...")
        services = [
            'cloudbilling.googleapis.com',
            'cloudapis.googleapis.com',
            'clouddebugger.googleapis.com',
            'cloudfunctions.googleapis.com',
            'cloudkms.googleapis.com',
            'cloudresourcemanager.googleapis.com',
            'cloudtrace.googleapis.com',
            'compute.googleapis.com',
            'deploymentmanager.googleapis.com',
            'genomics.googleapis.com',
            'iam.googleapis.com',
            'iamcredentials.googleapis.com',
            'logging.googleapis.com',
            'servicemanagement.googleapis.com',
            'storage-component.googleapis.com',
            'storage-api.googleapis.com'
        ]
        for service in services:
            cmd = 'gcloud --project {project} services enable {service}'.format(
                project=ld_project_for_namespace(project_id),
                service=service
            )
            print(cmd)
            subprocess.check_call(cmd, shell=True)
        cmd = (
            'gcloud beta billing projects link {project_id} --billing-account '
            '{billing_id}'.format(
                project_id=ld_project_for_namespace(project_id),
                billing_id=billing_id
            )
        )
        print("Enabling billing")
        print(cmd)
        subprocess.check_call(cmd, shell=True)
        print("Creating Signing Key")
        cmd = (
            'gcloud --project {project} kms keyrings create lapdog --location us'.format(
                project=ld_project_for_namespace(project_id)
            )
        )
        print(cmd)
        result = run_cmd(cmd)
        if result.returncode != 0 and b'ALREADY_EXISTS' not in result.buffer:
            raise ValueError("Unable to create Keyring")
        cmd = (
            'gcloud --project {project} alpha kms keys create lapdog-sign --location us --keyring'
            ' lapdog --purpose asymmetric-signing --default-algorithm '
            'rsa-sign-pss-3072-sha256 --protection-level software'.format(
                project=ld_project_for_namespace(project_id)
            )
        )
        print(cmd)
        result = run_cmd(cmd)
        if result.returncode != 0 and b'ALREADY_EXISTS' not in result.buffer:
            raise ValueError("Unable to create signing key")

        print("Creating Lapdog Roles")
        roles_url = "https://iam.googleapis.com/v1/projects/{project}/roles".format(
            project=ld_project_for_namespace(project_id)
        )
        user_session = generate_user_session(get_access_token())
        print("POST", roles_url)
        response = user_session.post(
            roles_url,
            headers={
                'Content-Type': 'application/json'
            },
            json={
                "roleId": "Pet_account",
                "role": {
                    "title": "Pet_account",
                    "includedPermissions": [
                        "cloudkms.cryptoKeyVersions.viewPublicKey",
                        "resourcemanager.projects.get",
                        "genomics.operations.cancel",
                        "genomics.operations.create",
                        "genomics.operations.get",
                        "genomics.operations.list",
                        "serviceusage.services.use" # for requester pays
                    ],
                    "stage": "GA"
                }
            }
        )
        if response.status_code != 200 and 'ALREADY_EXISTS' not in response.text:
            print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
            raise ValueError("Invalid response from Google API")
        print("POST", roles_url)
        response = user_session.post(
            roles_url,
            headers={
                'Content-Type': 'application/json'
            },
            json={
                "roleId": "Core_account",
                "role": {
                    "title": "Core_account",
                    "includedPermissions": [
                        "cloudkms.cryptoKeyVersions.useToSign",
                        "cloudkms.cryptoKeyVersions.viewPublicKey",
                        "resourcemanager.projects.get",
                        "genomics.datasets.create",
                        "genomics.datasets.delete",
                        "genomics.datasets.get",
                        "genomics.datasets.list",
                        "genomics.datasets.update",
                        "genomics.operations.cancel",
                        "genomics.operations.create",
                        "genomics.operations.get",
                        "genomics.operations.list"
                    ],
                    "stage": "GA"
                }
            }
        )
        if response.status_code != 200 and 'ALREADY_EXISTS' not in response.text:
            print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
            raise ValueError("Invalid response from Google API")
        print("POST", roles_url)
        response = user_session.post(
            roles_url,
            headers={
                'Content-Type': 'application/json'
            },
            json={
                "roleId": "Functions_account",
                "role": {
                    "title": "Functions_account",
                    "includedPermissions": [
                        "cloudkms.cryptoKeyVersions.viewPublicKey",
                        "iam.serviceAccountKeys.create",
                        "iam.serviceAccountKeys.delete",
                        "iam.serviceAccountKeys.get",
                        "iam.serviceAccountKeys.list",
                        "iam.serviceAccounts.create",
                        "iam.serviceAccounts.delete",
                        "iam.serviceAccounts.get",
                        "iam.serviceAccounts.getIamPolicy",
                        "iam.serviceAccounts.list",
                        "iam.serviceAccounts.setIamPolicy",
                        "iam.serviceAccounts.update",
                        "resourcemanager.projects.get",
                        "genomics.operations.create",
                        "genomics.operations.get",
                        "genomics.operations.list",
                        "resourcemanager.projects.getIamPolicy",
                        "resourcemanager.projects.setIamPolicy",
                        "compute.projects.get",
                        "compute.regions.get"
                    ],
                    "stage": "GA"
                }
            }
        )
        if response.status_code != 200 and 'ALREADY_EXISTS' not in response.text:
            print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
            raise ValueError("Invalid response from Google API")
        print("POST", roles_url)
        response = user_session.post(
            roles_url,
            headers={
                'Content-Type': 'application/json'
            },
            json={
                "roleId": "Lapdog_user",
                "role": {
                    "title": "Lapdog_user",
                    "includedPermissions": [
                        "genomics.operations.get",
                    ],
                    "stage": "GA"
                }
            }
        )
        if response.status_code != 200 and 'ALREADY_EXISTS' not in response.text:
            print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
            raise ValueError("Invalid response from Google API")

        print("Creating Core Service Account")
        cmd = (
            'gcloud --project {project} iam service-accounts create lapdog-worker --display-name lapdog-worker'.format(
                project=ld_project_for_namespace(project_id)
            )
        )
        print(cmd)
        result = run_cmd(cmd)
        if result.returncode != 0 and b'already exists' not in result.buffer:
            raise ValueError("Unable to create service account")
        core_account = 'lapdog-worker@{}.iam.gserviceaccount.com'.format(ld_project_for_namespace(project_id))
        print("Creating Cloud Functions Service Account")
        cmd = (
            'gcloud --project {project} iam service-accounts create lapdog-functions --display-name lapdog-functions'.format(
                project=ld_project_for_namespace(project_id)
            )
        )
        print(cmd)
        result = run_cmd(cmd)
        if result.returncode != 0 and b'already exists' not in result.buffer:
            raise ValueError("Unable to create service account")
        functions_account = 'lapdog-functions@{}.iam.gserviceaccount.com'.format(ld_project_for_namespace(project_id))
        print("Creating Metadata bucket while service accounts are created")
        cmd = (
            'gsutil mb -c Standard -l us-central1 -p {project} gs://{bucket}'.format(
                project=ld_project_for_namespace(project_id),
                bucket=ld_meta_bucket_for_project(ld_project_for_namespace(project_id))
            )
        )
        print(cmd)
        result = run_cmd(cmd)
        if result.returncode != 0 and b'already exists' not in result.buffer:
            raise ValueError("Unable to create bucket")
        print("Updating project IAM policy while service accounts are created")
        policy = {
            'serviceAccount:'+core_account: 'Core_account',
            'serviceAccount:'+functions_account: 'Functions_account'
        }
        print(policy)
        status, response = update_iam_policy(
            user_session,
            policy,
            ld_project_for_namespace(project_id)
        )
        if not status:
            print('(%d) : %s' % (response.status_code, response.text), file=sys.stderr)
            raise ValueError("Invalid Response from Google API")
        print("Waiting for service account creation...")
        time.sleep(30)
        print("Issuing Core Service Account Key")
        generate_core_key(ld_project_for_namespace(project_id), user_session)
        print("Updating Key Metadata ACL")
        blob = getblob(
            'gs://{bucket}/auth_key.json'.format(
                bucket=ld_meta_bucket_for_project(ld_project_for_namespace(project_id))
            )
        )
        acl = blob.acl
        for entity in acl.get_entities():
            if entity.type == 'project':
                if entity.identifier.startswith('editors-'):
                    entity.revoke_owner()
                elif entity.identifier.startswith('viewers-'):
                    entity.revoke_read()
        acl.user(functions_account).grant_read()
        acl.save()

        print("Deploying Cloud Functions")
        from .cloud import _deploy
        _deploy('create_submission', 'submit', functions_account, ld_project_for_namespace(project_id))
        _deploy('abort_submission', 'abort', functions_account, ld_project_for_namespace(project_id))
        _deploy('check_abort', 'signature', functions_account, ld_project_for_namespace(project_id))
        _deploy('register', 'register', functions_account, ld_project_for_namespace(project_id))
        _deploy('query_account', 'query', functions_account, ld_project_for_namespace(project_id))
        _deploy('quotas', 'quotas', functions_account, ld_project_for_namespace(project_id))
        _deploy('existence', 'existence', functions_account, ld_project_for_namespace(project_id))

    @property
    def registered(self):
        """
        Verifies that the current user is registered with this gateway
        """
        response = requests.post(
            self.get_endpoint('query'),
            headers={'Content-Type': 'application/json'},
            json={
                'token': get_access_token(),
            }
        )
        return response.status_code == 200

    def register(self, workspace, bucket):
        """
        Registers the currently logged in account with the api
        Must provide a workspace name and the associated bucket to prove that you have access
        to at least one workspace for this namespace
        """
        warnings.warn("[BETA] Gateway Register")
        response = requests.post(
            self.get_endpoint('register'),
            headers={'Content-Type': 'application/json'},
            json={
                'token': get_access_token(),
                'bucket': bucket,
                'namespace': self.namespace,
                'workspace': workspace,
            }
        )
        if response.status_code != 200:
            print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
            raise ValueError("Gateway failed to register user")
        return response.text # your account email

    def create_submission(self, workspace, bucket, submission_id, workflow_options=None, memory=3):
        """
        Sends a request through the lapdog execution API to start a new submission.
        Takes the local submission ID.
        Assumes the following files to be in place:
        Workflow Inputs : gs://{workspace bucket}/lapdog-executions/{submission id}/config.json
        Workflow WDL : gs://{workspace bucket}/lapdog-executions/{submission id}/method.wdl
        Submission JSON: gs://{workspace bucket}/lapdog-executions/{submission id}/submission.json

        The user will already have needed access to the workspace in order to evaluate
        workflow inputs.
        The user must have been granted access to the lapdog execution API for this
        workspace's namespace in order for the API to accept the request.
        The user's service account must have access to the workspace in order to
        download the input files specified in the workflow inputs.
        """
        warnings.warn("[BETA] Gateway Create Submission")
        response = requests.post(
            self.get_endpoint('submit'),
            headers={'Content-Type': 'application/json'},
            json={
                'token': get_access_token(),
                'bucket': bucket,
                'submission_id': submission_id,
                'namespace': self.namespace,
                'workspace': workspace,
                'workflow_options': workflow_options if workflow_options is not None else {},
                'memory': memory*1024
            }
        )
        if response.status_code == 200:
            operation = response.text
            submission_data_path = 'gs://{bucket}/lapdog-executions/{submission_id}/submission.json'.format(
                bucket=bucket,
                submission_id=submission_id
            )
            blob = getblob(submission_data_path)

            blob.upload_from_string(
                json.dumps(
                    {
                        **json.loads(blob.download_as_string().decode()),
                        **{'operation': operation}
                    }
                ).encode()
            )
            return True, operation
        return False, response


    def abort_submission(self, bucket, submission_id, hard=False):
        """
        Sends a request through the lapdog execution API to abort a running submission.
        Takes the local submission ID and a list of operations corresponding to
        the workflow calls
        Assumes the following file to be in place:
        Submission JSON: gs://{workspace bucket}/lapdog-executions/{submission id}/submission.json

        Cancels the cromwell server operation then cancels all workflow operations,
        then deletes all workflow machines, then finally the cromwell machine.
        """
        warnings.warn("[BETA] Gateway Abort Submission")
        response = requests.delete(
            self.get_endpoint('abort'),
            headers={'Content-Type': 'application/json'},
            json={
                'token': get_access_token(),
                'bucket': bucket,
                'submission_id': submission_id,
                'hard': hard
            }
        )
        if response.status_code != 200:
            return response

    def get_endpoint(self, endpoint):
        """
        1) Generates the appropriate url for a given endpoint in this project
        2) Checks that the endpoint exists by submitting an OPTIONS request
        3) Returns the full endpoint url
        """
        if endpoint not in __API_VERSION__:
            raise KeyError("Endpoint not defined: "+endpoint)
        endpoint_url = 'https://us-central1-{project}.cloudfunctions.net/{endpoint}-{version}'.format(
            project=self.project,
            endpoint=quote(endpoint),
            version=__API_VERSION__[endpoint]
        )
        response = requests.options(endpoint_url)
        if response.status_code == 204:
            return endpoint_url
        if response.status_code == 200 or response.status_code == 404:
            print("Lapdog Engine Project", self.project, "for namespace", self.namespace, "does not support api version", __API_VERSION__[endpoint], file=sys.stderr)
            if endpoint =='existence':
                raise ValueError("The existence endpoint could not be found. Project %s may not be initialized. Please contact the namespace admin" % self.project)
            raise ValueError("The project api for %s does not support %s version %s. Please contact the namespace admin" % (
                self.project,
                endpoint,
                __API_VERSION__[endpoint]
            ))
        raise ValueError("Unexpected status (%d) when checking for endpoint" % response.status_code)

    @property
    def exists(self):
        try:
            return requests.get(self.get_endpoint('existence')).status_code == 200
        except ValueError:
            return False

    @property
    def quota_usage(self):
        warnings.warn("[BETA] Gateway Quotas")
        response = requests.post(
            self.get_endpoint('quotas'),
            headers={'Content-Type': 'application/json'},
            json={
                'token': get_access_token()
            }
        )
        if response.status_code == 200:
            return response.json()
        print("Quota error (%d) : %s" % (response.status_code, response.text), file=sys.stderr)
        raise ValueError("Unable to fetch quotas: status %d" % response.status_code)
