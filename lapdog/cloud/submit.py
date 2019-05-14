import os
try:
    from . import utils
    from .utils import __API_VERSION__, __CROMWELL_TAG__, GCP_ZONES
except ImportError:
    import sys
    sys.path.append(os.path.dirname(__file__))
    import utils
    from utils import __API_VERSION__, __CROMWELL_TAG__, GCP_ZONES
import math
import json
import traceback
from dalmatian import getblob

@utils.cors('POST')
def create_submission(request):
    try:
        data = request.get_json()

        # 1) Validate the token

        if not isinstance(data, dict):
            return (
                {
                    'error': "Bad Request",
                    'message': ("No data was provided" if data is None else "Expected JSON dictionary in request body")
                },
                400
            )

        if 'token' not in data:
            return (
                {
                    'error': 'Bad Request',
                    'message': 'Missing required parameter "token"'
                },
                400
            )

        token_data = utils.get_token_info(data['token'])
        if 'error' in token_data:
            return (
                {
                    'error': 'Invalid Token',
                    'message': token_data['error_description'] if 'error_description' in token_data else 'Google rejected the client token'
                },
                401
            )

        # 1.b) Verify the user has a pet account
        response = utils.query_service_account(
            utils.generate_default_session(scopes=['https://www.googleapis.com/auth/cloud-platform']),
            utils.ld_acct_in_project(token_data['email'])
        )
        if response.status_code != 200:
            return (
                {
                    'error': 'User has not registered with this Lapdog Engine',
                    'message': response.text
                },
                401
            )

        # 2) Validate user's permission for the bucket

        if 'bucket' not in data:
            return (
                {
                    'error': 'Bad Request',
                    'message': 'Missing required parameter "bucket"'
                },
                400
            )

        session = utils.generate_user_session(data['token'])

        read, write = utils.validate_permissions(session, data['bucket'])
        if read is None:
            # Error, write will contain a message
            return (
                {
                    'error': 'Cannot Validate Bucket Permissions',
                    'message': write
                },
                400
            )
        if not (read and write):
            # User doesn't have full permissions to the bucket
            return (
                {
                    'error': 'Not Authorized',
                    'message': 'User lacks read/write permissions to the requested bucket'
                },
                401
            )

        # 2.b) Verify that the bucket belongs to this project

        if 'namespace' not in data or 'workspace' not in data:
            return (
                {
                    'error': 'Bad Request',
                    'message': 'Missing required parameters "namespace" and "workspace"'
                },
                400
            )
        core_session = utils.generate_core_session()

        result, message = utils.authenticate_bucket(
            data['bucket'], data['namespace'], data['workspace'], session, core_session
        )
        if not result:
            return (
                {
                    'error': 'Cannot Validate Bucket Signature',
                    'message': message
                },
                400
            )

        # 3) Check that submission.json exists, and is less than 1 Gib

        if 'submission_id' not in data:
            return (
                {
                    'error': 'Bad Request',
                    'message': 'Missing required parameter "submission_id"'
                },
                400
            )

        submission = utils.fetch_submission_blob(session, data['bucket'], data['submission_id'])

        result, message = utils.validate_submission_file(submission)
        if not result:
            return (
                {
                    'error': 'Bad Submission',
                    'message': message
                },
                400
            )

        # 4) Submit pipelines request

        region = 'us-central1'
        if 'compute_region' in data:
            allowed_regions = utils.enabled_regions()
            if data['compute_region'] in allowed_regions:
                region = data['compute_region']
            else:
                return (
                    {
                        'error': "Invalid Region",
                        'message': "Region not allowed. Enabled regions: " + repr(allowed_regions)
                    },
                    400
                )

        if 'memory' in data and data['memory'] > 3072:
            mtype = 'custom-%d-%d' % (
                math.ceil(data['memory']/13312)*2,
                data['memory']
            )
        else:
            mtype = 'n1-standard-1'

        pipeline = {
            'pipeline': {
                'actions': [
                    {
                        'imageUri': 'gcr.io/broad-cga-aarong-gtex/wdl_runner:' + __CROMWELL_TAG__,
                        'commands': [
                            '/wdl_runner/wdl_runner.sh'
                        ],
                        'environment': {
                            'SIGNATURE_ENDPOINT': 'https://{region}-{project}.cloudfunctions.net/signature-{version}'.format(
                                region=os.environ.get('FUNCTION_REGION'),
                                project=os.environ.get("GCP_PROJECT"),
                                version=__API_VERSION__['signature']
                            ),
                            'LAPDOG_PROJECT': os.environ.get('GCP_PROJECT'),
                            'WDL': "gs://{bucket}/lapdog-executions/{submission_id}/method.wdl".format(
                                bucket=data['bucket'],
                                submission_id=data['submission_id']
                            ),
                            'WORKFLOW_INPUTS': "gs://{bucket}/lapdog-executions/{submission_id}/config.tsv".format(
                                bucket=data['bucket'],
                                submission_id=data['submission_id']
                            ),
                            'WORKFLOW_OPTIONS': json.dumps(data['options']) if 'options' in data else '{}',
                            'LAPDOG_SUBMISSION_ID': data['submission_id'],
                            'WORKSPACE': "gs://{bucket}/lapdog-executions/{submission_id}/workspace/".format(
                                bucket=data['bucket'],
                                submission_id=data['submission_id']
                            ),
                            'OUTPUTS': "gs://{bucket}/lapdog-executions/{submission_id}/results".format(
                                bucket=data['bucket'],
                                submission_id=data['submission_id']
                            ),
                            'SUBMISSION_DATA_PATH': "gs://{bucket}/lapdog-executions/{submission_id}/submission.json".format(
                                bucket=data['bucket'],
                                submission_id=data['submission_id']
                            ),
                            'LAPDOG_LOG_PATH': "gs://{bucket}/lapdog-executions/{submission_id}/logs".format(
                                bucket=data['bucket'],
                                submission_id=data['submission_id']
                            ),
                            'PRIVATE_ACCESS': 'true' if ('no_ip' in data and data['no_ip']) else 'false',
                            'SUBMISSION_ZONES': " ".join(
                                '{}-{}'.format(region, zone)
                                for zone in GCP_ZONES[region]
                            )
                        }
                    }
                ],
                'resources': {
                    'projectId': os.environ.get("GCP_PROJECT"),
                    'regions': [region],
                    'virtualMachine': {
                        'machineType': mtype,
                        'preemptible': False,
                        'labels': {
                            'lapdog-execution-role': 'cromwell',
                            'lapdog-submission-id': data['submission_id']
                        },
                        'serviceAccount': {
                            'email': utils.ld_acct_in_project(token_data['email']),
                            'scopes': [
                                "https://www.googleapis.com/auth/cloud-platform",
                                "https://www.googleapis.com/auth/compute",
                                "https://www.googleapis.com/auth/devstorage.read_write",
                                "https://www.googleapis.com/auth/genomics"
                            ]
                        },
                        'bootDiskSizeGb': 20,
                        'network': {
                            'name': 'default',
                            'usePrivateAddress': ('no_ip' in data and data['no_ip'])
                        }
                    }
                },
            }
        }
        print(pipeline)
        response = utils.generate_default_session(
            [
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/compute",
                "https://www.googleapis.com/auth/genomics"
            ]
        ).post(
            'https://genomics.googleapis.com/v2alpha1/pipelines:run',
            headers={
                'Content-Type': 'application/json'
            },
            json=pipeline
        )
        try:
            if response.status_code == 200:
                operation = response.json()['name']

                # 5) Sign the operation

                utils.sign_object(
                    (data['submission_id'] + operation).encode(),
                    getblob(
                        'gs://{bucket}/lapdog-executions/{submission_id}/signature'.format(
                            bucket=data['bucket'],
                            submission_id=data['submission_id']
                        ),
                        credentials=session.credentials
                    ),
                    core_session.credentials
                )

                return operation, 200
        except:
            traceback.print_exc()
            return (
                {
                    'error': 'Unable to start submission',
                    'message': traceback.format_exc()
                },
                500
            )
        return (
            {
                'error': 'Unable to start submission',
                'message': 'Google rejected the pipeline request (%d) : %s' % (response.status_code, response.text)
            },
            400
        )
    except:
        traceback.print_exc()
        return (
            {
                'error': 'Unknown Error',
                'message': traceback.format_exc()
            },
            500
        )
