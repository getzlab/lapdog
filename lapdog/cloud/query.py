import os
try:
    from . import utils
except ImportError:
    import sys
    sys.path.append(os.path.dirname(__file__))
    import utils
import traceback

@utils.cors('POST')
def query_account(request):
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

        # 2) Check service account
        default_session = utils.generate_default_session(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        account_email = utils.ld_acct_in_project(token_data['email'])
        response = utils.query_service_account(default_session, account_email)
        if response.status_code >= 400:
            return (
                {
                    'error': 'Unable to query service account',
                    'message': response.text
                },
                400
            )
        if response.json()['email'] != account_email:
            return (
                {
                    'error': 'Service account email did not match expected value',
                    'message': response.json()['email'] + ' != ' + account_email
                },
                400
            )
        return account_email, 200

    except:
        traceback.print_exc()
        return (
            {
                'error': 'Unknown Error',
                'message': traceback.format_exc()
            },
            500
        )
