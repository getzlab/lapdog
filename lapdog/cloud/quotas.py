import os
try:
    from . import utils
except ImportError:
    import sys
    sys.path.append(os.path.dirname(__file__))
    import utils
import traceback

@utils.cors('POST')
def quotas(request):
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

        # 3) Query quota usage
        project_usage = default_session.get(
            'https://www.googleapis.com/compute/v1/projects/{project}'.format(
                project=os.environ.get('GCP_PROJECT')
            )
        )
        if project_usage.status_code != 200:
            return (
                {
                    'error': 'Invalid response from Google',
                    'message': '(%d) : %s' % (
                        project_usage.status_code,
                        project_usage.text
                    )
                },
                400
            )
        quotas = [
            {
                **quota,
                **{
                    'percent':  ('%0.2f%%' % (100 * quota['usage'] / quota['limit'])) if quota['limit'] > 0 else '0.00%'
                }
            }
            for quota in project_usage.json()['quotas']
        ]
        for region_name in utils.enabled_regions():
            region_usage = default_session.get(
                'https://www.googleapis.com/compute/v1/projects/{project}/regions/{region}'.format(
                    project=os.environ.get('GCP_PROJECT'),
                    region=region_name
                )
            )
            if region_usage.status_code != 200:
                return (
                    {
                        'error': 'Invalid response from Google',
                        'message': '(%d) : %s' % (
                            region_usage.status_code,
                            region_usage.text
                        )
                    },
                    400
                    )
            quotas += [
                {
                    **quota,
                    **{
                        'percent':  ('%0.2f%%' % (100 * quota['usage'] / quota['limit'])) if quota['limit'] > 0 else '0.00%',
                        'metric': region_name+'.'+quota['metric']
                    }
                }
                for quota in region_usage.json()['quotas']
            ]
        return (
            {
                'raw': quotas,
                'alerts': [quota for quota in quotas if quota['limit'] > 0 and quota['usage']/quota['limit'] >= 0.5]
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
