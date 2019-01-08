# This module defines utilities for the cloud function api

import subprocess
import requests
import os
import json

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
    # remember, the pipelines request must specify that user's service account
    # Move lapdog.gateway.get_account_in_project here.
    # Cromwell runner needs to handle fucking logs now, too
    # 1) Validate the token
    # 2) Check that the workspace and submission data path are the same bucket
    # 3) Validate the user's permissions for that bucket
    # 4) Check that the submission data path follows format gs://{bucket}/lapdog-executions/{submission_id}/submission.json
    # 5) Check that the path exists and is less than 1 Gib
    # 6) Submit pipelines request
    data = {
        'inputs': {
            'bucket': "<workspace bucket>",
            'submission_id': "<submission id>",
            'options': {"<Options JSON object"},
        },
        'token': '<user auth token>'
    }


    pipeline = {
        'pipeline': {
            'actions': [
                {
                    'imageUri': 'gcr.io/broad-cga-aarong-gtex/wdl_runner:v0.9.0',
                    'commands': [
                        '/wdl_runner/wdl_runner.sh'
                    ],
                    'environment': {
                        'WDL': "gs://{bucket}/lapdog-executions/{submission_id}/method.wdl".format(
                            bucket=data['inputs']['bucket'],
                            submission_id=data['inputs']['submission_id']
                        ),
                        'WORKFLOW_INPUTS': "gs://{bucket}/lapdog-executions/{submission_id}/config.tsv".format(
                            bucket=data['inputs']['bucket'],
                            submission_id=data['inputs']['submission_id']
                        ),
                        'WORKFLOW_OPTIONS': json.dumps(data['inputs']['options']),
                        'LAPDOG_SUBMISSION_ID': data['inputs']['submission_id'],
                        'WORKSPACE': "gs://{bucket}/lapdog-executions/{submission_id}/workspace/".format(
                            bucket=data['inputs']['bucket'],
                            submission_id=data['inputs']['submission_id']
                        ),
                        'OUTPUTS': "gs://{bucket}/lapdog-executions/{submission_id}/results".format(
                            bucket=data['inputs']['bucket'],
                            submission_id=data['inputs']['submission_id']
                        ),
                        'SUBMISSION_DATA_PATH': "gs://{bucket}/lapdog-executions/{submission_id}/submission.json".format(
                            bucket=data['inputs']['bucket'],
                            submission_id=data['inputs']['submission_id']
                        ),
                        'LAPDOG_LOG_PATH': "gs://{bucket}/lapdog-executions/{submission_id}/logs/".format(
                            bucket=data['inputs']['bucket'],
                            submission_id=data['inputs']['submission_id']
                        ),
                    }
                }
            ],
            'resources': {
                'projectId': os.environ.get("GCP_PROJECT"),
                'regions': ['us-central1'], # FIXME
                'virtualMachine': {
                    'machineType': 'n1-standard-1' or 'custom-2-{memory}' or 'custom-2-{memory}-ext', # FIXME
                    'preemptible': False,
                    'labels': {
                        'lapdog-execution-role': 'cromwell',
                        'lapdog-submission-id': data['inputs']['submission_id']
                    },
                    'serviceAccount': {
                        'email': FIXME_get_user_account(get_token_info(token)['email']),
                        'scopes': [
                            #TBD
                        ]
                    },
                    'bootDiskSizeGb': 20
                }
            },
        }
    }
    response = requests.post(
        'https://content-genomics.googleapis.com/v2alpha1/pipelines:run?alt=json',
        headers={
            'Authorization': 'Bearer ' + token
        },
        data=pipeline
    )
    try:
        return response.json()['name']

def get_token_info(token):
    try:
        data = requests.get('https://www.googleapis.com/oauth2/v1/tokeninfo?access_token='+token).json()
        return data
    except:
        return None

def validate_permissions(token, bucket):
    try:
        response = requests.get(
            "https://www.googleapis.com/storage/v1/b/{bucket}"
            "/iam/testPermissions?permissions=storage.objects.list&"
            "permissions=storage.objects.get&permissions=storage.objects.create&"
            "permissions=storage.objects.delete".format(bucket=bucket),
            headers={
                'Authorization': 'Bearer {token}'.format(token=token)
            }
        )
        if response.status_code == 200:
            data = response.json()
            if 'permissions' in data:
                return (
                    'storage.objects.list' in data['permissions'] or 'storage.objects.get' in data['permissions'],
                    'storage.objects.create' in data['permissions'] and 'storage.objects.delete' in data['permissions']
                )
            else:
                return False, False
        elif response.status_code == 401:
            data = response.json()
            if 'error' in data and 'message' in data['error'] and data['error']['message'] == 'Invalid Credentials':
                return None, 'Expired Credentials'
        return None, 'Invalid Credentials'
    except:
        return None, 'Unexpected Error'
