import os
try:
    from . import utils
except ImportError:
    import sys
    sys.path.append(os.path.dirname(__file__))
    import utils
import traceback
import sys
import json
from hashlib import sha512
from dalmatian import getblob

@utils.cors("POST")
def insert_resolution(request):
    """
    This function is unique. It is not deployed into each project.
    It is deployed once into my personal project which serves as a centralized
    database.
    """
    try:
        data = request.get_json()

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

        if 'namespace' not in data:
            return (
                {
                    'error': 'Missing Parameters',
                    'message': "Missing required parameter \"namespace\""
                },
                400
            )

        user_session = utils.generate_user_session(data['token'])

        while True:
            response = user_session.get(
                'https://api.firecloud.org/api/profile/billing'
            )

            if response.status_code == 200:
                break
            print(response.status_code, response.text, file=sys.stderr)
            if response.status_code == 404:
                return (
                    {
                        'error': "User not found",
                        'message': "You are not registered yet with firecloud"
                    },
                    404
                )
            time.sleep(5)


        projects = {proj['projectName']:proj for proj in response.json()}
        if data['namespace'] not in projects:
            return (
                {
                    'error': "Bad Namespace",
                    'message': 'The provided namespace "%s" could not be found' % data['namespace']
                },
                400
            )

        if projects[data['namespace']]['role'] not in {'User', 'Owner', 'Admin', 'Administrator'}:
            return (
                {
                    'error': "Insufficient Permissions",
                    'message': "The user lacks required privilages on the provided namespace"
                },
                401
            )

        if 'project' not in data:
            return (
                {
                    'error': "Missing parameters",
                    'message': "Missing required parameter \"project\""
                },
                400
            )

        response = user_session.post(
            'https://cloudresourcemanager.googleapis.com/v1/projects/{project}:getIamPolicy'.format(
                project=data['project']
            )
        )

        if response.status_code == 403:
            return (
                {
                    'error': 'Unauthorized',
                    'message': "User lacks permissions on the provided project"
                },
                401
            )

        if response.status_code != 200:
            return (
                {
                    'error': 'Unexpected response from Googla API',
                    'message': '(%d) : %s' % (response.status_code, response.text)
                },
                400
            )

        for policy in response.json()['bindings']:
            if policy['role'] == 'roles/owner':
                if ('user:'+token_data['email']) in policy['members']:
                    blob = getblob(
                        'gs://lapdog-resolutions/%s' % sha512(data['namespace'].encode()).hexdigest(),
                        credentials=utils.generate_default_session().credentials
                    )
                    if blob.exists():
                        return (
                            {
                                'error': "Already Exists",
                                'message': "A resolution for this namespace is already in place"
                            },
                            409
                        )
                    blob.upload_from_string(
                        data['project'].encode()
                    )
                    return (
                        'gs://lapdog-resolutions/%s' % sha512(data['namespace'].encode()).hexdigest(),
                        200
                    )
        return (
            {
                'error': "Unauthorized",
                'message': "User lacks ownership of the provided project"
            },
            400
        )

    except:
        return (
            {
                'error': "Unknown Error",
                'message': traceback.format_exc()
            },
            500
        )
