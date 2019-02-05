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
from .utils import ld_project_for_namespace, __API_VERSION__
from . import _deploy
from .. import __version__

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
