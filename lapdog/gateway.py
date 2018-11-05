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

@lru_cache()
def ld_project_for_namespace(namespace):
    # TEMP
    warnings.warn("Project for namespace returns constant")
    return 'broad-cga-aarong-gtex'
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
    expiry = int(cache_fetch('token', 'expiry', token=md5(token.encode()).hexdigest()))
    if not expiry:
        try:
            data = requests.get('https://www.googleapis.com/oauth2/v1/tokeninfo?access_token='+token).json()
            expiry = int(time.time() + int(data['expires_in']))
            cache_write(str(expiry), 'token', 'expiry', token=md5(token.encode()).hexdigest())
        except:
            return True
    print(expiry < time.time(), expiry - time.time())
    return expiry < time.time()

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
        # TODO
        print("TODO : ")
