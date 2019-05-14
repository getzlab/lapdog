# This module defines utilities for the cloud function api

from .utils import __API_VERSION__
import tempfile
import shutil
import subprocess
import glob
import os

__FUNCTION_MAPPING__ = {
    'create_submission': 'submit.py',
    'abort_submission': 'abort.py',
    'existence': 'internal.py',
    'redacted': 'internal.py',
    'check_abort': 'internal.py',
    'quotas': 'quotas.py',
    'register': 'register.py',
    'query_account': 'query.py',
    'insert_resolution': 'resolution.py'
}

RESOLUTION_URL = "https://us-central1-a-graubert.cloudfunctions.net/resolve-" + __API_VERSION__['resolve']

def _deploy(function, endpoint, service_account=None, project=None, overload_version=None):
    if overload_version is None:
        overload_version = __API_VERSION__[endpoint]
    with tempfile.TemporaryDirectory() as tempdir:
        with open(os.path.join(tempdir, 'requirements.txt'), 'w') as w:
            w.write('google-auth\n')
            w.write('google-cloud-storage\n')
            w.write('google-cloud-kms\n')
            w.write('cryptography\n')
            w.write('firecloud-dalmatian>=0.0.7\n')
        shutil.copyfile(
            os.path.join(
                os.path.dirname(__file__),
                __FUNCTION_MAPPING__[function]
            ),
            os.path.join(tempdir, 'main.py')
        )
        shutil.copyfile(
            os.path.join(
                os.path.dirname(__file__),
                'utils.py'
            ),
            os.path.join(tempdir, 'utils.py')
        )
        cmd = 'gcloud {project} beta functions deploy {endpoint}-{version} --entry-point {function} --runtime python37 --trigger-http --source {path} {service_account}'.format(
            endpoint=endpoint,
            version=overload_version,
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
