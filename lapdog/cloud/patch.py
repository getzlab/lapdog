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
from .utils import ld_project_for_namespace, __API_VERSION__, generate_user_session, ld_meta_bucket_for_project, getblob
from . import _deploy, RESOLUTION_URL
from .. import __version__
from ..gateway import get_access_token, resolve_project_for_namespace
from ..lapdog import WorkspaceManager
import sys
import crayons
import traceback
from firecloud import api as fc
from hashlib import sha512
import requests

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
    'abort-alpha',
    'abort-beta',
    'query-alpha',
    'query-beta',
    'quotas-alpha',
    'quotas-beta',
    'signature-alpha',
    'signature-beta'
]

def __project_admin_apply_patch(namespace):
    """
    PATCH SPEC: Beta -> V1
    """
    print("Patch version", __version__)
    print("Patching Namespace:", namespace)
    user_session = generate_user_session(get_access_token())
    print(crayons.black("Phase 1/4:", bold=True), "Checking resolution status")
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
    print("Lapdog Project:", project)
    functions_account = 'lapdog-functions@{}.iam.gserviceaccount.com'.format(project)
    roles_url = "https://iam.googleapis.com/v1/projects/{project}/roles".format(
        project=project
    )
    print(crayons.black("Phase 2/4:", bold=True), "Update IAM Policies")
    print("PATCH", roles_url+"/Core_account")
    response = user_session.patch(
        roles_url+"/Core_account",
        headers={
            'Content-Type': 'application/json'
        },
        json={
            "title": "Core_account",
            "includedPermissions": [
                "cloudkms.cryptoKeyVersions.useToSign",
                "cloudkms.cryptoKeyVersions.viewPublicKey",
                "cloudkms.cryptoKeyVersions.get",
                "cloudkms.cryptoKeyVersions.list",
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
    )
    if response.status_code != 200:
        print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
        raise ValueError("Invalid response from Google API")
    print("PATCH", roles_url+"/Functions_account")
    response = user_session.patch(
        roles_url+"/Functions_account",
        headers={
            'Content-Type': 'application/json'
        },
        json={
            "title": "Functions_account",
            "includedPermissions": [
                "cloudkms.cryptoKeyVersions.viewPublicKey",
                "cloudkms.cryptoKeyVersions.get",
                "cloudkms.cryptoKeyVersions.list",
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
    )
    if response.status_code != 200:
        print("(%d) : %s" % (response.status_code, response.text), file=sys.stderr)
        raise ValueError("Invalid response from Google API")
    print(crayons.black("Phase 3/4:", bold=True), "Deploy Cloud API Updates")
    print("Patching submit from v1 -> v2")
    _deploy('create_submission', 'submit', functions_account, project)
    print("Patching abort from beta -> v1")
    _deploy('abort_submission', 'abort', functions_account, project)
    print("Patching signature from beta -> v1")
    _deploy('check_abort', 'signature', functions_account, project)
    print("Patching register from v1 -> v2")
    _deploy('register', 'register', functions_account, project)
    print("Patching query from beta -> v1")
    _deploy('query_account', 'query', functions_account, project)
    print("Patching quotas from beta -> v1")
    _deploy('quotas', 'quotas', functions_account, project)
    print(crayons.black("Phase 4/4:", bold=True), "Redact Insecure Cloud API Endpoints")
    response = user_session.get(
        'https://cloudfunctions.googleapis.com/v1/projects/{project}/locations/us-central1/functions'.format(
            project=project
        )
    )
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
    print(crayons.black("%d insecure endpoints detected"%len(redactions), bold=True))
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
