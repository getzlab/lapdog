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
            signature.encode(),
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
                        'commands': [
                            (
                                'bash -c "git clone $LAPDOG_CLONE_URL && '
                                'cd lapdog && git checkout $LAPDOG_TAG && '
                                'python3 -m pip install -e . && '
                                'lapdog apply-patch $LAPDOG_NAMESPACE" > stdout.log 2> stderr.log && '
                                'gsutil cp stdout.log stderr.log $LAPDOG_LOG_PATH'
                            )
                        ],
                        'environment': {
                            'LAPDOG_PROJECT': os.environ.get('GCP_PROJECT'),
                            'LAPDOG_LOG_PATH': "gs://{bucket}/update-logs/{tag}/".format(
                                bucket=utils.ld_meta_bucket_for_project(),
                                tag=data['tag']
                            ),
                            'LAPDOG_CLONE_URL': data['url'],
                            'LAPDOG_NAMESPACE': {},
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
                            'lapdog-update-tag': data['tag']
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
    This will be deployed to the central project
    It responds to git webhook updates and triggers the self update mechanism in other projects
    Remember to set the SECRET environment variable after deployment
    Deploy to endpoint: webhook
    If updating the UPDATE endpoint: wait for webhook to trigger updates first,
    then redeploy webhook
    """
    # 1) Check that the X-Hub-Signature is valid for the request
    # This ensures that the request is actually coming from our github webhook
    try:
        if 'X-Hub-Signature' not in request.headers:
            return {
                'error': 'Missing Signature',
                'message': "The required X-Hub-Signature header was not provided"
            }, 400
        signature = request.headers['X-Hub-Signature']
        if signature.startswith('sha1='):
            signature = signature[5:]
        try:
            digest = hmac.digest(os.environ['SECRET'].encode(), request.get_data(), 'sha1').hex()
        except KeyError:
            return {
                'error': 'Missing Secret',
                'message': 'The secret environment variable was not set'
            }, 500
        if signature != digest:
            return {
                'error': "Bad Signature",
                'message': "The provided SHA1 HMAC signature did not match the known secret"
            }, 401

        # 2) Get required data from payload
        data = request.get_json()
        if 'zen' in data and 'hook_id' in data:
            # this is a ping event
            return "Thanks!", 200
        if 'ref_type' not in data:
            return {
                'error': 'Missing Parameters',
                'message': "Missing required payload parameter 'ref_type'"
            }, 400
        if data['ref_type'] != 'tag':
            return "Ignored. Updates not triggered for new branches", 200
        if 'ref' not in data:
            return {
                'error': 'Missing Parameters',
                'message': "Missing required payload parameter 'ref'"
            }, 400
        if 'repository' not in data:
            return {
                'error': 'Missing Parameters',
                'message': "Missing required payload parameter 'repository'"
            }, 400
        if data['repository']['clone_url'] != 'https://github.com/broadinstitute/lapdog.git':
            return {
                'error': 'Invalid URL',
                'message': "The 'clone_url' passed to the webhook has changed. Please redeploy the webhook with the new expected value"
            }, 409
        update_payload = {
            'random': os.urandom(16).hex(),
            'tag': data['ref'],
            'url': data['repository']['clone_url']
        }

        # 3) Get all resolved namespaces and update the iam policy for the signing key
        default_session = utils.generate_default_session()
        resolutions = [
            blob.download_as_string().decode()
            for page in utils._getblob_client(default_session.credentials).bucket('lapdog-resolutions').list_blobs(fields='items/name,nextPageToken').pages
            for blob in page
        ]
        success, response = utils.update_iam_policy(
            default_session,
            {
                'serviceAccount:lapdog-functions@{}.iam.gserviceaccount.com'.format(resolution): 'signingKeyVerifier'
                for resolution in resolutions
            }
        )

        if not success:
            return {
                'error': 'Unable to update IAM policy',
                'message': "Google rejected the policy update: (%d) : %s" % (response.status_code, response.text)
            }, 500

        # 4) Trigger update for all policies
        status = {
            'results': []
        }
        failed = 200
        signature = utils._get_signature(json.dumps(update_payload).encode(), utils.UPDATE_KEY_PATH, default_session.credentials)
        for resolution in resolutions:
            try:
                response = requests.post(
                    'https://us-central1-{project}.cloudfunctions.net/update-{version}'.format(
                        project=resolution,
                        version=utils.__API_VERSION__['update']
                    ),
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
