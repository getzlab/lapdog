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
from .utils import ld_project_for_namespace, __API_VERSION__, generate_user_session
from . import _deploy
from .. import __version__
from ..gateway import get_access_token
import sys
import crayons

# Notes for creating future patches
# 1) Import a Gateway and use get_endpoint to check for any redacted endpoints
#       Redact any endpoints which can be found
# 2) Always redeploy the current latest version of all endpoints/role definitions/api bindings
# 3) Mark the separate phases (iam, deploy, redact)

# Alternatively, write a function for each patch
# When running patches, check that the previous patch has been applied

def __project_admin_apply_patch(namespace):
    """
    PATCH SPEC: Beta -> V1
    """
    print("Patch version", __version__)
    print("Patching Namespace:", namespace)
    project = ld_project_for_namespace(namespace)
    print("Lapdog Project:", project)
    functions_account = 'lapdog-functions@{}.iam.gserviceaccount.com'.format(project)
    roles_url = "https://iam.googleapis.com/v1/projects/{project}/roles".format(
        project=project
    )
    print(crayons.black("Phase 1/3:", bold=True), "Update IAM Policies")
    user_session = generate_user_session(get_access_token())
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
    print(crayons.black("Phase 2/3:", bold=True), "Deploy Cloud API Updates")
    print("Patching submit from beta -> v1")
    _deploy('create_submission', 'submit', functions_account, project)
    print("Patching abort from beta -> v1")
    _deploy('abort_submission', 'abort', functions_account, project)
    print("Patching signature from beta -> v1")
    _deploy('check_abort', 'signature', functions_account, project)
    print("Patching register from beta -> v1")
    _deploy('register', 'register', functions_account, project)
    print("Patching query from beta -> v1")
    _deploy('query_account', 'query', functions_account, project)
    print("Patching quotas from beta -> v1")
    _deploy('quotas', 'quotas', functions_account, project)
    print(crayons.black("Phase 3/3:", bold=True), "Redact Insecure Cloud API Endpoints")
