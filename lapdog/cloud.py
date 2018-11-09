# This module defines utilities for the cloud function api

import subprocess

def cloud_api_endpoint(request):
    if request.method == 'POST':
        data = request.get_json()
        # Global request format
        # {
        #     'auth': "<Issuer's auth token>",
        #     'method': "<Lapdog API method name>",
        #     'args': ['...'],
        #     'kwargs': {'...'}
        # }

def create_submission(request):

    data = {
        'pileline-file': '<Pipeline file text>',
        'zones': '<Compute zone>',
        'inputs': {
            'WDL': '<gs path to method>',
            'WORKFLOW_INPUTS': '<gs path to config>',
            'WORKFLOW_OPTIONS': {
                '<Options JSON object>'
            },
            'LAPDOG_SUBMISSION_ID': '<Submission id>',
            'WORKSPACE': '<gs path to submission workspace>',
            'OUTPUTS': '<gs path to submission outputs>',
            'SUBMISSION_DATA_PATH': '<gs path to submission.json>'
        },
        'logging': '<gs path to submission logs folder>',
        'labels': '<labels for cromwell>',
        'token': '<user auth token>''
    }
    cmd = (
        'gcloud alpha genomics pipelines run '
        '--pipeline-file {source_dir}/wdl_pipeline.yaml '
        '--zones {zone} '
        '--inputs WDL={wdl_text} '
        '--inputs WORKFLOW_INPUTS={workflow_template} '
        '--inputs-from-file WORKFLOW_OPTIONS={options_template} '
        '--inputs LAPDOG_SUBMISSION_ID={submission_id} '
        '--inputs WORKSPACE=gs://{bucket_id}/lapdog-executions/{submission_id}/workspace '
        '--inputs OUTPUTS=gs://{bucket_id}/lapdog-executions/{submission_id}/results '
        '--inputs SUBMISSION_DATA_PATH={submission_data_path} '
        '--logging gs://{bucket_id}/lapdog-executions/{submission_id}/logs '
        '--labels lapdog-submission-id={submission_id},lapdog-execution-role=cromwell '
        '--service-account-scopes=https://www.googleapis.com/auth/devstorage.read_write'
    )
