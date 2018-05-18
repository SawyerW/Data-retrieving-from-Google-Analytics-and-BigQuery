import argparse
import sys

import httplib2
from apiclient import discovery
from apiclient.discovery import build
from oauth2client import client
from oauth2client import file
from oauth2client import tools


def GetService_analytics():
    """Get a service that communicates to a Google API.

    Args:
      api_name: string The name of the api to connect to.
      api_version: string The api version to connect to.
      scope: A list of strings representing the auth scopes to authorize for the
        connection.
      client_secrets_path: string A path to a valid client secrets file.

    Returns:
      A service that is connected to the specified API.
    """
    client_secrets_path = 'client_secrets.json'
    api_version = 'v3'
    api_name = 'analytics'
    scope = ['https://www.googleapis.com/auth/analytics.edit']

    # Parse command-line arguments.
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])
    flags = parser.parse_args([])

    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
        client_secrets_path, scope=scope,
        message=tools.message_if_missing(client_secrets_path))

    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    TOKEN_FILE_NAME = 'credentials_edit.dat'
    storage = file.Storage(TOKEN_FILE_NAME)
    credentials = storage.locked_get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=httplib2.Http())

    # Build the service object.
    service = build(api_name, api_version, http=http)
    print (service)
    return service
