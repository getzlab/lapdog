import os
import base64
import webbrowser
import json
import time
import subprocess
from threading import RLock
from urllib.parse import urlencode
from hashlib import sha256
from getpass import getuser
import requests
import crayons
from google.auth.transport.requests import AuthorizedSession
from  google.oauth2.credentials import Credentials
from .cloud import utils
from .cache import cached

@cached(60, 1)
def get_gcloud_account():
    """
    Gets the currently logged in gcloud account.
    This checks main credentials (gcloud auth login)
    Not application default credentials used by lapdog (gcloud auth application-default login)
    60 second cache (because user can change sign-in)
    """
    return subprocess.run(
        'gcloud config get-value account',
        shell=True,
        stdout=subprocess.PIPE
    ).stdout.decode().strip()

class AuthenticationError(ValueError):
    pass

class InvalidToken(AuthenticationError):
    pass

class AccountMismatch(AuthenticationError):
    pass

class BadDomain(AuthenticationError):
    pass

class LapdogToken(object):
    SCOPES = [
        'email',
        'profile',
        'openid',
        'https://www.googleapis.com/auth/devstorage.read_write'
    ]

    def __init__(self, account=None):
        """
        Represents a self-refreshing access token for Lapdog APIs, issued by Google.
        Credentials are saved to disk, so each account should only need to login once.
        Access token is valid for 1 hour, but can be refreshed with .refresh()
        """
        self.account = account if account is not None else get_gcloud_account()
        if not self.account.endswith('@broadinstitute.org'):
            raise BadDomain("Non-broad emails are currently unsupported by the Lapdog OAuth system")
        self.token = None
        self.refresh_token = None
        self.ident = None
        self._lock = RLock()
        self._session = None

        path = LapdogToken.path_for_account(self.account)
        try:
            with open(path) as w:
                data = json.load(w)

            self.token = data['access_token']
            self.refresh_token = data['refresh_token']
            self.ident = data['id_token']

        except (FileNotFoundError, json.decoder.JSONDecodeError):
            self.auto_login()

        if not self.valid:
            return self.refresh()

        if self.info['email'] != self.account:
            raise AccountMismatch("{} != {}".format(self.info['email'], self.account))

    @staticmethod
    def path_for_account(account):
        account = sha256(('66e21f0205b97437399244c1095452a0' + getuser() + account).encode()).hexdigest()
        base_path = os.path.expanduser(os.path.join(
            '~',
            '.config',
            'lapdog',
            'auth',
            account[:2],
            account[2:4]
        ))
        if not os.path.exists(base_path):
            os.makedirs(base_path)
        return os.path.join(
            base_path,
            account[4:]
        )

    @staticmethod
    @cached(10)
    def _validate(token, account):
        info = utils.get_token_info(token)
        return (
            'error' not in info
            and 'audience' in info
            and info['audience'] == utils.OAUTH_CLIENT_ID
            and 'expires_in' in info
            and info['expires_in'] > 10 # 10s is important. This way token will not expire during current cache window
            and 'email' in info
            and info['email'] == account
        )

    @property
    def valid(self):
        """
        Checks that the token is valid and not expired
        """
        return LapdogToken._validate(self.token, self.account)


    @property
    def auto_token(self):
        """
        Returns authentication token.
        If token expires in less than 60s, refresh it.
        If token not logged in, raise Authentication Error
        """
        if not (self.valid and self.info['expires_in'] >= 60):
            self.refresh()
        return self.token

    @property
    @cached(10)
    def info(self):
        """
        Get token info
        """
        return utils.get_token_info(self.token)

    def manual_login(self):
        """
        Manually login to the new token.
        Opens a browser tab for authentication, but requires the user manually
        copy-paste the authorization code into the terminal.
        Handles second-step authentication
        """
        pub, priv = self._generate_verifier()
        # refresh w/ redirect
        self._prompt(
            'urn:ietf:wg:oauth:2.0:oob',
            code_challenge=pub
        )
        # Get user to paste code
        self.authenticate(
            input("Paste authentication token: ").strip(),
            redirect_uri='urn:ietf:wg:oauth:2.0:oob',
            code_verifier=priv
        )

    def browser_login(self, uri):
        """
        Login to the new token by passing authorization code to the provided callback uri.
        Opens a browser tab for authentication, but code is transferred automatically.
        Does not handle second-step authentication.
        You must manually fetch the authorization code from the server which handled
        the callback uri, then call .authenticate()
        """
        pub, priv = self._generate_verifier()
        state = base64.urlsafe_b64encode(os.urandom(32)).decode()
        # refresh w/ redirect
        self._prompt(
            uri,
            code_challenge=pub,
            state=state
        )
        return priv, state

    def auto_login(self, port=4201):
        """
        Main entrypoint for token initialization.
        Checks if Lapdog UI is running, then uses that server for authentication.
        Falls back on manual copy-paste authentication
        """
        try:
            fetch_url = 'http://127.0.0.1:{}/api/v1/auth/fetch?state={{}}'.format(port)
            callback_url = 'http://127.0.0.1:{}/api/v1/auth/callback'.format(port)
            response = requests.get(
                fetch_url.format('marco')
            )
            if response.status_code == 200 and response.json() == 'polo':
                priv, state = self.browser_login(
                    callback_url
                )
                print("Waiting for user to accept browser prompt")
                response = requests.get(
                    fetch_url.format(state)
                )
                while response.status_code == 402:
                    time.sleep(1)
                    response = requests.get(
                        fetch_url.format(state)
                    )
                if response.status_code == 200:
                    return self.authenticate(
                        response.json(),
                        callback_url,
                        code_verifier=priv
                    )
                else:
                    raise AuthenticationError("Invalid response from client-side OAuth ({}) : {}".format(response.status_code, response.text))
        except requests.ConnectionError:
            # UI not running, so do manual login
            pass
        self.manual_login()

    def _generate_verifier(self):
        private_key = base64.urlsafe_b64encode(os.urandom(94)).rstrip(b'=')
        return base64.urlsafe_b64encode(sha256(private_key).digest()).decode().rstrip('='), private_key.decode()

    def _prompt(self, redirect_uri, state=None, code_challenge=None, login_hint=None):
        """
        Opens a browser tab for the user to authenticate
        """
        url = 'https://accounts.google.com/o/oauth2/v2/auth?'

        data = {
            'client_id': utils.OAUTH_CLIENT_ID,
            'redirect_uri': redirect_uri,
            'response_type': 'code',
            'scope': ' '.join(LapdogToken.SCOPES),
        }

        if state is not None:
            data['state'] = state

        if code_challenge is not None:
            data['code_challenge_method'] = 'S256'
            data['code_challenge'] = code_challenge

        if login_hint is not None:
             data['login_hint'] = login_hint

        if not webbrowser.open_new_tab(
            url + urlencode(data)
        ):
            print(
                crayons.red("Unable to open browser."),
                'Please visit:',
                crayons.normal(url + urlencode(data), bold=True)
            )

    def authenticate(self, code, redirect_uri, code_verifier=None):
        """
        Performs final authentication. Authorization code is exchanged for
        access token and refresh token
        """
        data = {
            'code': code,
            'client_id': utils.OAUTH_CLIENT_ID,
            'redirect_uri': redirect_uri,
            'grant_type': 'authorization_code'
        }

        if code_verifier is not None:
            data['code_verifier'] = code_verifier

        response = requests.post(
            utils.AUTHENTICATION_URL,
            headers={
                'Content-Type': 'application/json'
            },
            json=data
        )

        if response.status_code == 200:
            auth_data = response.json()
            if LapdogToken._validate(auth_data['access_token'], self.account):
                self._save_info(
                    token=auth_data['access_token'],
                    refresh=auth_data['refresh_token'],
                    ident=auth_data['id_token'] if 'id_token' in auth_data else None
                )
            else:
                raise InvalidToken("Unable to obtain oauth token for this account")
        else:
            raise AuthenticationError("Invalid response from server-side OAuth ({}) : {}".format(response.status_code, response.text))


    def refresh(self, t=None):
        """
        Refreshes a token, assuming the client already has a refresh_token
        """
        if t is not None and self.info['expires_in'] > t:
            # If user specifies a time, only refresh if the token expires in the given window
            return
        response = requests.post(
            utils.AUTHENTICATION_URL,
            headers={
                'Content-Type': 'application/json'
            },
            json={
                'refresh_token': self.refresh_token,
                'client_id': utils.OAUTH_CLIENT_ID,
                'grant_type': 'refresh_token'
            }
        )

        if response.status_code == 200:
            auth_data = response.json()
            if LapdogToken._validate(auth_data['access_token'], self.account):
                self._save_info(
                    token=auth_data['access_token'],
                    refresh=self.refresh_token,
                    ident=auth_data['id_token'] if 'id_token' in auth_data else None
                )
            else:
                self.auto_login()
        else:
            raise AuthenticationError("Invalid response from server-side OAuth ({}) : {}".format(response.status_code, response.text))

    def _credentials(self):
        """
        Return valid credentials object
        """
        return Credentials(
            self.auto_token,
            id_token=self.ident,
            scopes=LapdogToken.SCOPES
        )

    @property
    def authorized_session(self):
        """
        Return an active AuthorizedSession for this user
        """
        with self._lock:
            if self._session is None or self._session.credentials.token != self.token or not self.valid:
                self._session = AuthorizedSession(
                    self._credentials(),
                    refresh_status_codes=[],
                )
            return self._session

    def _save_info(self, token, refresh, ident=None):
        info = utils.get_token_info(token)

        if 'error' in info or 'audience' not in info or info['audience'] != utils.OAUTH_CLIENT_ID or 'expires_in' not in info or info['expires_in'] < 10 or 'email' not in info:
            raise InvalidToken("Invalid token {}".format(repr(info)))

        with open(LapdogToken.path_for_account(info['email']), 'w') as w:
            json.dump({
                'access_token': token,
                'refresh_token': refresh,
                'id_token': ident
            }, w)

        if info['email'] != self.account:
            raise AccountMismatch("{} != {}".format(info['email'], self.account))

        self.token = token
        self.refresh_token = refresh
        self.ident = ident
