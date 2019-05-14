import os
try:
    from . import utils
except ImportError:
    import sys
    sys.path.append(os.path.dirname(__file__))
    import utils
import json
from urllib.parse import quote
import traceback
from dalmatian import getblob

@utils.cors('DELETE')
def abort_submission(request):
    try:

        # https://genomics.googleapis.com/google.genomics.v2alpha1.Pipelines

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

        # 4) Download submission and parse operation

        try:
            submission = json.loads(submission.download_as_string().decode())
        except:
            return (
                {
                    'error': 'Invalid Submission',
                    'message': 'Submission was not valid JSON'
                },
                400
            )

        if 'operation' not in submission:
            return (
                {
                    'error': 'Invalid Submission',
                    'message': 'Submission contained no operation metadata'
                },
                400
            )

        signature_blob = getblob(
            'gs://{bucket}/lapdog-executions/{submission_id}/signature'.format(
                bucket=data['bucket'],
                submission_id=data['submission_id']
            ),
            credentials=session.credentials
        )
        if not signature_blob.exists():
            return (
                {
                    'error': 'No Signature',
                    'message': 'The submission signature could not be found. Refusing to abort job'
                },
                403
            )

        if not utils.verify_signature(signature_blob, (data['submission_id'] + submission['operation']).encode()):
            return (
                {
                    'error': 'Invalid Signature',
                    'message': 'Could not validate submission signature. Refusing to abort job'
                },
                403
            )

        core_session = utils.generate_core_session()

        # 5) Generate abort key

        utils.sign_object(
            data['submission_id'].encode(),
            getblob(
                'gs://{bucket}/lapdog-executions/{submission_id}/abort-key'.format(
                    bucket=data['bucket'],
                    submission_id=data['submission_id']
                ),
                credentials=session.credentials
            ),
            core_session.credentials
        )

        if 'hard' in data and data['hard']:
            # 6) Abort operation
            response = core_session.post(
                "https://genomics.googleapis.com/v2alpha1/{operation}:cancel".format(
                    operation=quote(submission['operation']) # Do not quote slashes here
                )
            )

            return response.text, response.status_code
        return (
            {
                'status': 'Aborting',
                'message': 'A soft-abort request has been sent.'
                ' If the submission does not abort soon, abort it with hard=True to force-kill the cromwell server'
            },
            200
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
