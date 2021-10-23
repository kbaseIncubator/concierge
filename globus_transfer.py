#!/usr/bin/env python

import json
import sys
from globus_sdk import (NativeAppAuthClient,
                        RefreshTokenAuthorizer,
                        TransferClient,
                        TransferData)
from globus_sdk.exc import GlobusAPIError


class GlobusTransfer:
    def __init__(self, user, params):
        self.token_file = params['TokenFile']
        self.client_id = params['ClientId']
        self.dest_endpt = params['DestinationEndpoint']
        self.src_endpt = params['SourceEndpoint']
        self.user = user
        self._load_tokens_from_file()

    def _load_tokens_from_file(self):
        """Load a set of saved tokens."""
        with open(self.token_file, "r") as f:
            self.tokens = json.load(f)
        self.transfer_tokens = self.tokens["transfer.api.globus.org"]

    def _save_tokens_to_file(self, tokens):
        """Save a set of tokens for later use."""
        with open(self.token_file, "w") as f:
            json.dump(tokens, f)

    def _update_tokens_file_on_refresh(self, token_response):
        """
        Callback function passed into the RefreshTokenAuthorizer.
        Will be invoked any time a new access token is fetched.
        """
        self._save_tokens_to_file(self.token_file, token_response.by_resource_server)


    def transfer(self, flist):

        auth_client = NativeAppAuthClient(client_id=self.client_id)

        authorizer = RefreshTokenAuthorizer(
            self.transfer_tokens["refresh_token"],
            auth_client,
            access_token=self.transfer_tokens["access_token"],
            expires_at=self.transfer_tokens["expires_at_seconds"],
            on_refresh=self._update_tokens_file_on_refresh,
        )

        transfer = TransferClient(authorizer=authorizer)

        try:
            transfer.endpoint_autoactivate(self.src_endpt)
            transfer.endpoint_autoactivate(self.dest_endpt)
        except GlobusAPIError as ex:
            print(ex)
            if ex.http_status == 401:
                sys.exit(
                    "Refresh token has expired. "
                    "Please delete refresh-tokens.json and try again."
                )
            else:
                raise ex

        tdata = TransferData(transfer,
                             self.src_endpt,
                             self.dest_endpt,
                             label="Automated Transfers",
                             sync_level="checksum")
        for srcfile in flist:
            fn = srcfile.split('/')[-1]
            tdata.add_item(srcfile, "/%s/%s" % (self.user, fn))
        transfer_result = transfer.submit_transfer(tdata)
        print("task_id =", transfer_result["task_id"])


#if __name__ == "__main__":
#    transfer(sys.argv[1:])
