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

id_rsa = os.path.join(
    os.path.expanduser('~'),
    '.ssh',
    'id_rsa'
)

@lru_cache()
def ld_project_for_namespace(namespace):
    # TEMP
    warnings.warn("Project for namespace returns constant")
    return 'broad-cga-aarong-gtex' # Note: we can
    prefix = ('ld-'+namespace)[:25]
    suffix = md5(prefix.encode()).hexdigest().lower()
    return prefix + '-' + suffix[:4]

def ld_acct_in_project(account, ld_project):
    # TEMP
    warnings.warn("Account for project returns constant")
    return 'lapdog-worker@broad-cga-aarong-gtex.iam.gserviceaccount.com'
    # Use a regex replace. can only contain lowercase alphanumeric characters and hyphens
    return 'lapdog-'+account.split('@')[0] + '@' + ld_project + '.iam.gserviceaccount.com'

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
        token = subprocess.run(
            'gcloud auth application-default print-access-token',
            shell=True,
            stdout=subprocess.PIPE
        ).stdout.decode().strip()
        cache_write(token, 'token', 'access-token', md5(account.encode()).hexdigest(), google='credentials')
    return token

def get_token_expired(token):
    expiry = cache_fetch('token', 'expiry', token=md5(token.encode()).hexdigest())
    if expiry is None:
        try:
            data = requests.get('https://www.googleapis.com/oauth2/v1/tokeninfo?access_token='+token).json()
            expiry = int(time.time() + int(data['expires_in']))
            cache_write(str(expiry), 'token', 'expiry', token=md5(token.encode()).hexdigest())
        except:
            return True
    return int(expiry) < time.time()

class Gateway(object):
    """Acts as an interface between local lapdog and any resources behind the project's API"""

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
        # subprocess.check_call(cmd, shell=True)
        cmd = (
            'gcloud beta billing projects link {project_id} --billing-account '
            '{billing_id}'.format(
                project_id=ld_project_for_namespace(project_id),
                billing_id=billing_id
            )
        )
        print("Enabling billing")
        print(cmd)
        # subprocess.check_call(cmd, shell=True)
        # TODO
        print("TODO : ADD CLOUD FUNCTIONS")
        print("TODO : ADD ROOT SERVICE ACCOUNT")
        print("TODO : SETUP LAPDOG ROLES")
        Gateway.grant_access_to_user(
            project_id,
            get_account(),
            True
        )

    @classmethod
    def grant_access_to_user(cls, project_id, target_account, is_moderator=False):
        """
        Grants the listed account access to the lapdog execution API for the given
        firecloud project. NOTE: This does not grant the user access to any data
        in any workspaces. This allocates a new service account for the user and
        grants required permissions to interact with the API.

        If is_moderator is set to True, the user will also have permissions to grant
        other users access to the lapdog execution API
        """
        print("TODO : GRANT ROLES TO TARGET")
        print("TODO : ISSUE SERVICE ACCOUNT FOR TARGET")
        print("TODO : Create user-execution-group in firecloud if not already exists")
        print("TODO : Add new service account to user-execution-group")

    def create_submission(self, submission_id, workflow_options=None):
        """
        Sends a request through the lapdog execution API to start a new submission.
        Takes the local submission ID and (optionally) the workflow options as a dictionary.
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
        print("TODO : Format and submit request to api")

    def abort_submission(self, submission_id, operations):
        """
        Sends a request through the lapdog execution API to abort a running submission.
        Takes the local submission ID and a list of operations corresponding to
        the workflow calls
        Assumes the following file to be in place:
        Submission JSON: gs://{workspace bucket}/lapdog-executions/{submission id}/submission.json

        Cancels the cromwell server operation then cancels all workflow operations,
        then deletes all workflow machines, then finally the cromwell machine.
        """
        print("TODO: Format and submit requesteroni to the api")

    def monitor_submission(self, submission_id):
        """
        Sends a request through the lapdog execution API to allow the user to connect
        to a cromwell instance to monitor the logs.
        Takes the local submission ID.
        Assumes the following file to be in place:
        Submission JSON: gs://{workspace bucket}/lapdog-executions/{submission id}/submission.json
        Requires that the user has an ssh identity set up at ~/.ssh/id_rsa

        Sends the user's public key (~/.ssh/id_rsa.pub) to the API which copies
        it to the cromwell instance
        """
        instance_ip = cache_fetch(
            'cromwell-ip',
            self.workspace.namespace,
            self.workspace.workspace,
            submission_id,
            'ip-address'
        )
        if instance_ip is None:
            # Never connected to this instance before
            if not os.path.isfile(is_rsa):
                raise FileNotFoundError("No ssh key found. Please run 'ssh-keygen'")
            print("TODO : Submit id_rsa.pub to API")
            print("TODO : Save IP address to instance_ip variable")
            cache_write(
                instance_ip,
                'cromwell-ip',
                self.workspace.namespace,
                self.workspace.workspace,
                submission_id,
                'ip-address'
            )
        return instance_ip # `ssh -i ~/.ssh/id_rsa {instance_ip}`
