"""
This module is intended to contain code required to patch a given namespace to the current version of lapdog
Lapdog patch will run __project_admin_apply_patch
This function may:
* Deploy new cloud functions to endpoints with updated versions
* Delete insecure old versions
* Modify project roles
* Update iam policy or bindings
* Regenerate signing keys
"""
from .utils import ld_project_for_namespace, __API_VERSION__, generate_user_session, ld_meta_bucket_for_project
from . import _deploy, RESOLUTION_URL
from .. import __version__
from ..gateway import get_access_token, resolve_project_for_namespace, CORE_PERMISSIONS, FUNCTIONS_PERMISSIONS
from ..lapdog import WorkspaceManager
from dalmatian import getblob
import sys
import crayons
import traceback
from firecloud import api as fc
from hashlib import sha512
import requests
from uuid import uuid4

# Notes for creating future patches
# 1) Import a Gateway and use get_endpoint to check for any redacted endpoints
#       Redact any endpoints which can be found
# 2) Always redeploy the current latest version of all endpoints/role definitions/api bindings
# 3) Mark the separate phases (iam, deploy, redact)

# Alternatively, write a function for each patch
# When running patches, check that the previous patch has been applied

__REDACTIONS=[
    'register-alpha',
    'register-beta',
    'register-v1',
    'submit-alpha',
    'submit-beta',
    'submit-v1',
    'submit-v3a',
    'abort-alpha',
    'abort-beta',
    'query-alpha',
    'query-beta',
    'quotas-alpha',
    'quotas-beta',
    'signature-alpha',
    'signature-beta'
]

__ENDPOINTS__ = {
    'submit': 'create_submission',
    'abort': 'abort_submission',
    'signature': 'check_abort',
    'register': 'register',
    'query': 'query_account',
    'quotas': 'quotas'
}

def __project_admin_apply_patch(namespace):
    """
    PATCH SPEC: Beta -> V1
    """
    print("Patch version", __version__)
    print("Patching Namespace:", namespace)
    user_session = generate_user_session(get_access_token())
    print(crayons.normal("Phase 1/5:", bold=True), "Checking resolution status")
    blob = getblob(
        'gs://lapdog-resolutions/' + sha512(namespace.encode()).hexdigest()
    )
    if not blob.exists():
        print("Patching resolution")
        response = requests.post(
            RESOLUTION_URL,
            headers={"Content-Type": "application/json"},
            json={
                'token': get_access_token(),
                'namespace': namespace,
                'project': ld_project_for_namespace(namespace)
            }
        )
        if response.status_code == 409:
            proj = resolve_project_for_namespace(project_id)
            if proj != custom_lapdog_project:
                raise NameError("A resolution is already in place for this namespace. Please contact GitHub @agraubert")
        elif response.status_code != 200:
            print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
            raise ValueError("Error when generating resolution")
        project = ld_project_for_namespace(namespace)
    else:
        project = blob.download_as_string().decode()
        print(crayons.green("Remote Resolution intact"))
    print("Lapdog Project:", crayons.normal(project, bold=True))
    blob = getblob(
        'gs://{bucket}/resolution'.format(
            bucket = ld_meta_bucket_for_project(project)
        )
    )
    if not blob.exists():
        print("Patching resolution")
        blob.upload_from_string(namespace.encode())
        acl = blob.acl
        acl.all_authenticated().grant_read()
        acl.save()
    else:
        print(crayons.green("Local Resolution intact"))
    functions_account = 'lapdog-functions@{}.iam.gserviceaccount.com'.format(project)
    roles_url = "https://iam.googleapis.com/v1/projects/{project}/roles".format(
        project=project
    )
    print(crayons.normal("Phase 2/5:", bold=True), "Update IAM Policies")
    response = user_session.get(
        roles_url+"/Core_account"
    )
    if response.status_code == 404:
        print("Redeploying Policy")
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
                    "includedPermissions": CORE_PERMISSIONS,
                    "stage": "GA"
                }
            }
        )
    elif response.status_code == 200:
        perms = {*response.json()['includedPermissions']}
        if len(perms ^ set(CORE_PERMISSIONS)):
            print("Updating Policy")
            print("PATCH", roles_url+"/Core_account")
            response = user_session.patch(
                roles_url+"/Core_account",
                headers={
                    'Content-Type': 'application/json'
                },
                json={
                    "title": "Core_account",
                    "includedPermissions": CORE_PERMISSIONS,
                    "stage": "GA"
                }
            )
            if response.status_code != 200:
                print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
                raise ValueError("Invalid response from Google API")
        else:
            print(crayons.green("Policy OK"))
    else:
        print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
        raise ValueError("Invalid response from Google API")
    response = user_session.get(
        roles_url+"/Functions_account"
    )
    if response.status_code == 404:
        print("Redeploying Policy")
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
                    "includedPermissions": FUNCTIONS_PERMISSIONS,
                    "stage": "GA"
                }
            }
        )
    elif response.status_code == 200:
        perms = {*response.json()['includedPermissions']}
        if len(perms ^ set(FUNCTIONS_PERMISSIONS)):
            print("Updating Policy")
            print("PATCH", roles_url+"/Functions_account")
            response = user_session.patch(
                roles_url+"/Functions_account",
                headers={
                    'Content-Type': 'application/json'
                },
                json={
                    "title": "Functions_account",
                    "includedPermissions": FUNCTIONS_PERMISSIONS,
                    "stage": "GA"
                }
            )
            if response.status_code != 200:
                print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
                raise ValueError("Invalid response from Google API")
        else:
            print(crayons.green("Policy OK"))
    else:
        print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
        raise ValueError("Invalid response from Google API")
    print(crayons.normal("Phase 3/5:", bold=True), "Checking VPC Configuration")
    blob = getblob('gs://{bucket}/regions'.format(bucket=ld_meta_bucket_for_project(project)))
    regions = ['us-central1']
    try:
        if blob.exists():
            regions = blob.download_as_string().decode().split()
    except:
        pass
    for region in regions:
        subnet_url = "https://www.googleapis.com/compute/v1/projects/{project}/regions/{region}/subnetworks/default".format(
            project=project,
            region=region
        )
        response = user_session.get(subnet_url)
        if response.status_code != 200:
            raise ValueError("Unexpected response from Google (%d) : %s" % (response.status_code, response.text))
        subnet = response.json()
        if not ('privateIpGoogleAccess' in subnet and subnet['privateIpGoogleAccess']):
            print("Patching VPC Configuration in region", region)
            print("POST", subnet_url+'/setPrivateIpGoogleAccess')
            response = user_session.post(
                subnet_url+'/setPrivateIpGoogleAccess',
                headers={
                    'Content-Type': "application/json"
                },
                params={
                    'requestId': str(uuid4())
                },
                json={
                    "privateIpGoogleAccess": True
                }
            )
            if response.status_code >= 400:
                raise ValueError("Unexpected response from Google (%d) : %s" % (response.status_code, response.text))
        else:
            print(crayons.green("VPC Configuration Valid for region "+region))
    print(crayons.normal("Phase 4/5:", bold=True), "Deploy Cloud API Updates")
    response = user_session.get(
        'https://cloudfunctions.googleapis.com/v1/projects/{project}/locations/us-central1/functions'.format(
            project=project
        )
    )
    if response.status_code != 200:
        print("Unable to query existing functions. Redeploying all functions")
        deployments = {
            k:v
            for f,v in __API_VERSION__.items()
            if f != 'resolve'
        }
    else:
        functions = {
            function['name'].split('/')[-1]
            for function in response.json()['functions']
            if function['entryPoint'] != 'redacted'
        }
        deployments = {
            f:v
            for f,v in __API_VERSION__.items()
            if (f+'-'+v) not in functions and f != 'resolve'
        }
    if len(deployments):
        print("Deploying", len(deployments), "functions")
        for func, ver in deployments.items():
            print("Deploying endpoint", func, ver)
            _deploy(__ENDPOINTS__[func], func, functions_account, project)
    else:
        print(crayons.green("No updates"))
    print(crayons.normal("Phase 5/5:", bold=True), "Redact Insecure Cloud API Endpoints")
    if response.status_code != 200:
        print("Unable to query existing functions. Applying all redactions")
        redactions = __REDACTIONS
    else:
        redactions = [
            function['name'].split('/')[-1]
            for function in response.json()['functions']
            if function['name'].split('/')[-1] in __REDACTIONS
            and function['entryPoint'] != 'redacted'
        ]
    if len(redactions) == 0:
        print(crayons.green("All endpoints secure"))
        return
    print(crayons.normal("%d insecure endpoints detected"%len(redactions), bold=True))
    print("It is strongly recommended that you redact the insecure endpoints")
    print("Press Enter to redact endpoints, Ctrl+C to abort")
    try:
        input()
    except KeyboardInterrupt:
        print("Aborted Redaction Process")
        print("%d insecure cloud endpoints will remain active"%len(redactions))
        return
    for redaction in redactions:
        print(crayons.red("Redacting "+redaction))
        endpoint, version = redaction.split('-')
        _deploy('redacted', endpoint, functions_account, project, version)
