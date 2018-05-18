"""Access and manage a Google Tag Manager account."""

import argparse
import sys

import httplib2

from apiclient.discovery import build
from oauth2client import client
from oauth2client import file
from oauth2client import tools


def GetService(api_name, api_version, scope, client_secrets_path):
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
    storage = file.Storage(api_name + '.dat')
    print (storage)
    credentials = storage.get()
    print (credentials)
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=httplib2.Http())

    # Build the service object.
    service = build(api_name, api_version, http=http)

    return service


def FindGreetingsContainer(service, account_path):
    """Find the greetings container.

    Args:
      service: the Tag Manager service object.
      account_path: the path of the Tag Manager account from which to retrieve the
        Greetings container.

    Returns:
      The greetings container if it exists, or None if it does not.
    """
    # Query the Tag Manager API to list all containers for the given account.
    container_wrapper = service.accounts().containers().list(
        parent=account_path).execute()

    # Find and return the Greetings container if it exists.
    # for container in container_wrapper['container']:

    return container_wrapper

    # Find and return the Greetings container if it exists.
    # for container in container_wrapper['container']:
    #
    #     return container
    # return None



def CreateWorkspace(service, container,workspace_name):
    """Creates a workspace named 'my workspace'.

    Args:
      service: the Tag Manager service object.
      container: the container to insert the workspace within.

    Returns:
      The created workspace.
    """
    return service.accounts().containers().workspaces().create(
        parent=container['path'],
        body={
            'name': workspace_name,
        }).execute()


def CreateConversionLinkerTag(service, workspace):
    """Create the Universal Analytics Tag.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace to create a tag within.
      tracking_id: the Universal Analytics tracking ID to use.

    Returns:
      The created tag.
    """

    conversion_linker_tag = {
        'name': 'Conversion_linker',
        'type': 'gclidw'
    }

    return service.accounts().containers().workspaces().tags().create(
        parent=workspace['path'],
        body=conversion_linker_tag).execute()


def CreateConversionLinker(service, workspace):
    """Create the Trigger.

    Args:
      service: the Tag Manager service object.
      workspace: the workspace to create the trigger within.

    Returns:
      The created trigger.
    """

    all_pages_trigger = {
        'name': 'All_pages',
        'type': 'PAGEVIEW'
    }

    return service.accounts().containers().workspaces().triggers().create(
        parent=workspace['path'],
        body=all_pages_trigger).execute()


def UpdateConversionLinkerWithTrigger(service, tag, trigger):
    """Update a Tag with a Trigger.

    Args:
      service: the Tag Manager service object.
      tag: the tag to associate with the trigger.
      trigger: the trigger to associate with the tag.
    """
    # Get the tag to update.
    tag = service.accounts().containers().workspaces().tags().get(
        path=tag['path']).execute()

    # Update the Firing Trigger for the Tag.
    tag['firingTriggerId'] = [trigger['triggerId']]

    # Update the Tag.
    response = service.accounts().containers().workspaces().tags().update(
        path=tag['path'],
        body=tag).execute()

# def CreateVersionFromWorkspace(service,workspace):
#
#    response = service.accounts().containers().workspaces().create_version(
#        path=workspace['path'],
#        body={
#            'name' : 'API - randstad - tttt',
#            'notes' : 'tttt'
#        }
#    ).execute()
#
#    return response


def main():

    # Define the auth scopes to request.
    scope = ['https://www.googleapis.com/auth/tagmanager.edit.containers']

    # Authenticate and construct service.
    service = GetService('tagmanager', 'v2', scope, 'client_secrets.json')

    # Get tag manager account ID from command line.
    account_name_list = ['xxxxxx']
    account_path_list = ['xxxxx']

    # Account loop for the tag manager of aa admin
    for account_id in range(len(account_path_list)):

        account_path = 'accounts/' + account_path_list[account_id]

        # Find the greetings container.
        container = FindGreetingsContainer(service, account_path)

        # Container loop under specific account
        for container_entity in container['container']:
            tmp = []
            # print container_entity
            #Get list all workspaces in the container
            list = service.accounts().containers().workspaces().list(
                parent=container_entity['path']
            ).execute()
            # Workspace information
            list_workspace = list['workspace']

            # Extract names of all the workspaces
            for i in list_workspace:
                tmp.append(i['name'])
                # print i['name']
            # print all the workspace names of the container
            print (tmp)
            # Check if workspace named with "Expand Dev-team Workspace" exists
            # if exist, pass the container and will manually check later
            if "Expand Dev-team Workspace" in tmp:
                print ("Expand Dev-team Workspace already exists in container: " + container_entity['name'] + ". Under account: " + account_name_list[account_id])
                continue
            elif len(tmp) == 3:
                print ("There are already 3 workspaces in container: " + container_entity['name'] + ". Under account: " + account_name_list[account_id] + ". Please find a solution")
                continue
            else:
                try:

                    # Create a new workspace.
                    workspace = CreateWorkspace(service, container_entity, 'Conversion_linker_workspace')

                    # Create the  tag.
                    tag = CreateConversionLinkerTag(service, workspace)

                # Create the  Trigger.
                    trigger = CreateConversionLinker(service, workspace)

                # Update the tag to fire based on the tag.
                    UpdateConversionLinkerWithTrigger(service, tag, trigger)


                except:
                    print ("Problem happens while creating workspace in container: " + container_entity[
                        'name'] + ". Under account: " + account_name_list[account_id])
                    continue


if __name__ == '__main__':
    main()
