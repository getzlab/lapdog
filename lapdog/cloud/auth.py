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
import requests
from hashlib import sha512

@utils.cors("POST")
def oauth(request):
    """
    This function is unique. It is not deployed into each project.
    It is deployed once into my personal project which serves as a centralized
    database.

    This endpoint handles incoming OAuth
    """
    logger = utils.CloudLogger().log_request(request)
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

        if 'grant_type' not in data:
            return (
                {
                    'error': 'Missing Parameters',
                    'message': "Missing required 'grant_type' parameter"
                },
                400
            )

        if 'client_id' not in data:
            return (
                {
                    'error': 'Missing Parameters',
                    'message': "Missing required 'client_id' parameter"
                },
                400
            )

        if data['client_id'] != utils.OAUTH_CLIENT_ID:
            return (
                {
                    'error': 'Bad Client',
                    'message': 'The provided client ID did not match the server\'s client ID'
                },
                409
            )

        if data['grant_type'] == 'authorization_code':
            if 'code' not in data:
                return (
                    {
                        'error': 'Missing Parameters',
                        'message': "Missing required 'code' parameter (required by grant_type = authorization_code)"
                    },
                    400
                )
            if 'redirect_uri' not in data:
                return (
                    {
                        'error': 'Missing Parameters',
                        'message': "Missing required 'redirect_uri' parameter (required by grant_type = authorization_code)"
                    },
                    400
                )
            data['client_secret'] = os.environ['OAUTH_CLIENT_SECRET']
            response = requests.post(
                'https://oauth2.googleapis.com/token',
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                data=data
            )
            if response.status_code != 200:
                return (
                    {
                        'error': 'Authorization failed',
                        'message': 'Google rejected authorization request: {}'.format(response.text)
                    },
                    response.status_code
                )
            return response.json(), 200
        elif data['grant_type'] == 'refresh_token':
            if 'refresh_token' not in data:
                return (
                    {
                        'error': 'Missing Parameters',
                        'message': "Missing required 'refresh_token' parameter (required by grant_type = refresh_token)"
                    },
                    400
                )
            data['client_secret'] = os.environ['OAUTH_CLIENT_SECRET']
            response = requests.post(
                'https://oauth2.googleapis.com/token',
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                data=data
            )
            if response.status_code != 200:
                return (
                    {
                        'error': 'Refresh failed',
                        'message': 'Google rejected refresh request: {}'.format(response.text)
                    },
                    response.status_code
                )
            return response.json(), 200
        else:
            return (
                {
                    'error': 'Bad grant_type',
                    'message': 'grant_type must be "authorization_code" or "refresh_token"'
                },
                400
            )
    except:
        logger.log_exception()
        return (
            {
                'error': 'Unknown Error',
                'message': traceback.format_exc()
            },
            500
        )
