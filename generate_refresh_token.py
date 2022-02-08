#!/usr/bin/env python

"""
This was taken from the Gloubus example but modified a bit.

This is used to generate the refresh token.  This is generally
just run once to bootstrap things.
"""

import json
import sys
import time
import webbrowser
import os

from globus_sdk import NativeAppAuthClient, RefreshTokenAuthorizer, TransferClient
from globus_sdk.exc import GlobusAPIError

TOKEN_FILE = "refresh-tokens.json"
REDIRECT_URI = "https://auth.globus.org/v2/web/auth-code"
SCOPES = "openid email profile " "urn:globus:auth:scope:transfer.api.globus.org:all"


def is_remote_session():
    return os.environ.get("SSH_TTY", os.environ.get("SSH_CONNECTION"))

def load_tokens_from_file(filepath):
    """Load a set of saved tokens."""
    with open(filepath, "r") as f:
        tokens = json.load(f)

    return tokens


def save_tokens_to_file(filepath, tokens):
    """Save a set of tokens for later use."""
    with open(filepath, "w") as f:
        json.dump(tokens, f)


def do_native_app_authentication(client_id, redirect_uri, requested_scopes=None):
    """
    Does a Native App authentication flow and returns a
    dict of tokens keyed by service name.
    """
    client = NativeAppAuthClient(client_id=client_id)
    # pass refresh_tokens=True to request refresh tokens
    client.oauth2_start_flow(
        requested_scopes=requested_scopes,
        redirect_uri=redirect_uri,
        refresh_tokens=True,
    )

    url = client.oauth2_get_authorize_url()

    print("Native App Authorization URL: \n{}".format(url))

    if not is_remote_session():
        webbrowser.open(url, new=1)

    auth_code = input("Enter the auth code: ").strip()

    token_response = client.oauth2_exchange_code_for_tokens(auth_code)

    # return a set of tokens, organized by resource server name
    return token_response.by_resource_server


def main():
    CLIENT_ID=sys.argv[1]
    # if we need to get tokens, start the Native App authentication process
    tokens = do_native_app_authentication(CLIENT_ID, REDIRECT_URI, SCOPES)

    try:
        save_tokens_to_file(TOKEN_FILE, tokens)
    except:
        pass


if __name__ == "__main__":
    main()
