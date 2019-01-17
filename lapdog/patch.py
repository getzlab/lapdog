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
from .cloud import ld_project_for_namespace, _deploy, __API_VERSION__

_deploy(function, endpoint, service_account=None, project=None)

def __project_admin_apply_patch(namespace):
    """
    PATCH SPEC: Beta -> V1
    """
    project = ld_project_for_namespace(namespace)
    functions_account = 'lapdog-functions@{}.iam.gserviceaccount.com'.format(project)
    _deploy('create_submission', 'submit', functions_account, project)
    _deploy('abort_submission', 'abort', functions_account, project)
    _deploy('check_abort', 'signature', functions_account, project)
    _deploy('register', 'register', functions_account, project)
    _deploy('query', 'query', functions_account, project)
