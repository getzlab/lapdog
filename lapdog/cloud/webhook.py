import os
try:
    from . import utils
except ImportError:
    import sys
    sys.path.append(os.path.dirname(__file__))
    import utils
import base64
import hmac
import requests
import json
import time
import traceback

@utils.cors("POST")
def update(request):
    """
    Handles update request from the master update webhook
    """
    try:
        # 1) Validate the request
        if 'X-Lapdog-Signature' not in request.headers:
            return {
                'error': 'Missing Signature',
                'message': "The required X-Lapdog-Signature header was not provided"
            }, 400

        signature = request.headers['X-Lapdog-Signature']
        data = request.get_json()
        if not isinstance(data, dict):
            return (
                {
                    'error': "Bad Request",
                    'message': ("No data was provided" if data is None else "Expected JSON dictionary in request body")
                },
                400
            )
        result = utils.verify_signature(
            bytes.fromhex(signature),
            json.dumps(data).encode(),
            utils.UPDATE_KEY_PATH,
            _is_blob=False
        )
        if not result:
            return {
                'error': 'Bad Signature',
                'message': "The provided signature for this update was invalid"
            }, 401
        if not ('tag' in data and 'url' in data and 'random' in data):
            return {
                'error': 'Missing parameters',
                'message': 'Missing one or more of the required parameters "tag", "url", and "random"',
            }, 400

        # 2) Build pipeline to boot update VM

        regions = utils.enabled_regions()
        if len(regions) < 1:
            return {
                'error': 'No regions',
                'message': 'There are no regions enabled in this project'
            }, 503

        pipeline = {
            'pipeline': {
                'actions': [
                    {
                        'imageUri': 'gcr.io/broad-cga-aarong-gtex/self_update:' + utils.UPDATE_IMAGE_TAG,
                        'commands': ['/update.sh'],
                        'environment': {
                            'LAPDOG_PROJECT': os.environ.get('GCP_PROJECT'),
                            'LAPDOG_LOG_PATH': "gs://{bucket}/update-logs/{time}-{tag}/".format(
                                bucket=utils.ld_meta_bucket_for_project(),
                                tag=data['tag'],
                                time=int(time.time())
                            ),
                            'LAPDOG_CLONE_URL': data['url'],
                            'LAPDOG_NAMESPACE': utils.getblob(
                                'gs://{bucket}/resolution'.format(
                                    bucket=utils.ld_meta_bucket_for_project(os.environ.get('GCP_PROJECT'))
                                )
                            ).download_as_string().decode(),
                            'LAPDOG_TAG': data['tag']
                        }
                    }
                ],
                'resources': {
                    'projectId': os.environ.get("GCP_PROJECT"),
                    'regions': regions,
                    'virtualMachine': {
                        'machineType': 'f1-micro',
                        'preemptible': False,
                        'labels': {
                            'lapdog-execution-role': 'self-update',
                        },
                        'serviceAccount': {
                            'email': 'lapdog-update@{}.iam.gserviceaccount.com'.format(os.environ.get('GCP_PROJECT')),
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
                            'usePrivateAddress': False
                        }
                    }
                },
            }
        }

        # 3) Launch pipeline

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
                return response.json()['name'], 200
            return (
                {
                    'error': 'Unable to start update',
                    'message': 'Google rejected the pipeline request (%d) : %s' % (response.status_code, response.text)
                },
                400
            )
        except:
            traceback.print_exc()
            return (
                {
                    'error': 'Unable to start update',
                    'message': traceback.format_exc()
                },
                500
            )
    except:
        traceback.print_exc()
        return {
            'error': 'Unknown error',
            'message': traceback.format_exc()
        }, 500

@utils.cors("POST")
def webhook(request):
    """
    Admins: Use this to trigger a self-update to the given reference (commit, tag, or branch)
    Control who has access to trigger updates with the invoker permissions to the webhook
    """
    try:
        # 1) Check token details to ensure user in whitelist
        token = utils.extract_token(request.headers, None)
        if token is None:
            return {
                'error': 'No credentials',
                'message': 'User did not pass identity in Authorization header'
            }, 400
        token_data = utils.get_token_info(token)
        if 'error' in token_data:
            return (
                {
                    'error': 'Invalid Token',
                    'message': token_data['error_description'] if 'error_description' in token_data else 'Google rejected the client token'
                },
                401
            )
        if token_data['email'] not in os.environ['INVOKERS'].split(','):
            return {
                'error': 'Not authorized',
                'message': 'User "{}" not present in function environment configuration'.format(token_data['email'])
            }, 403
        data = request.get_json()
        if 'ref' not in data:
            return {
                'error': 'Missing parameters',
                'message': 'Missing required parameter "ref"',
            }, 400

        update_payload = {
            'random': os.urandom(16).hex(),
            'tag': data['ref'],
            'url': 'https://github.com/broadinstitute/lapdog.git'
        }

        # 2) Get all resolved namespaces and update the iam policy for the signing key
        default_session = utils.generate_default_session()
        resolutions = [
            blob.download_as_string().decode()
            for page in utils._getblob_client(default_session.credentials).bucket('lapdog-resolutions').list_blobs(fields='items/name,nextPageToken').pages
            for blob in page
        ]

        policy = default_session.get(
            'https://cloudkms.googleapis.com/v1/projects/broad-cga-aarong-gtex/locations/global/keyRings/lapdog:getIamPolicy'
        )
        if policy.status_code != 200:
            return {
                'error': 'Unable to read IAM policy',
                'message':response.text
            }, 500
        policy = policy.json()
        for i, binding in enumerate(policy['bindings']):
            if binding['role'] == 'projects/broad-cga-aarong-gtex/roles/signingKeyVerifier':
                policy['bindings'][i]['members'] = [
                    'serviceAccount:lapdog-functions@{}.iam.gserviceaccount.com'.format(resolution)
                    for resolution in resolutions
                ]
        response = default_session.post(
            'https://cloudkms.googleapis.com/v1/projects/broad-cga-aarong-gtex/locations/global/keyRings/lapdog:setIamPolicy',
            headers={'Content-Type': 'application/json'},
            json={
                "policy": policy,
                "updateMask": "bindings"
            }
        )
        if response.status_code != 200:
            return {
                'error': 'Unable to update IAM policy',
                'message': "Google rejected the policy update: (%d) : %s" % (response.status_code, response.text)
            }, 500

        # 3) Trigger update for all resolutions
        status = {
            'results': []
        }
        failed = 200
        signature = utils._get_signature(json.dumps(update_payload).encode(), utils.UPDATE_KEY_PATH, default_session.credentials)
        for resolution in resolutions:
            try:
                update_url = 'https://us-central1-{project}.cloudfunctions.net/update-{version}'.format(
                    project=resolution,
                    version=utils.__API_VERSION__['update']
                )
                if default_session.options(update_url).status_code == 204:
                    response = default_session.post(
                        update_url,
                        headers={
                            'Content-Type': 'application/json',
                            'X-Lapdog-Signature': signature.hex()
                        },
                        json=update_payload
                    )
                    failed = max(failed, response.status_code)
                    status['results'].append({
                        'project': resolution,
                        'status': 'OK' if response.status_code == 200 else 'Failed',
                        'message': response.text,
                        'code': response.status_code
                    })
                else:
                    status['results'].append({
                        'project': resolution,
                        'status': 'Error',
                        'message': "The target endpoint 'update-{}' does not exist".format(utils.__API_VERSION__['update']),
                        'code': 0
                    })
            except:
                failed = max(failed, 500)
                status['results'].append({
                    'project': resolution,
                    'status': 'Error',
                    'message': traceback.format_exc(),
                    'code': 0
                })

        return status, failed
    except:
        traceback.print_exc()
        return {
            'error': 'Unknown error',
            'message': traceback.format_exc()
        }, 500

def trigger_update(ref):
    """
    Admins: Use to easily trigger the self-update webhook
    """
    return utils.generate_default_session(["https://www.googleapis.com/auth/cloud-platform"]).post(
        'https://us-central1-broad-cga-aarong-gtex.cloudfunctions.net/webhook-v1',
        headers={
            'Content-Type': 'application/json',
        },
        json={
            'ref': ref
        }
    )
