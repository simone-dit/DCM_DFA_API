#!/usr/bin/python
#
# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This example illustrates how to get a list of all user profiles."""
import argparse
import sys

from googleapiclient import discovery
import httplib2
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

OAUTH_SCOPES = ['https://www.googleapis.com/auth/dfareporting']

CREDENTIAL_STORE_FILE = 'auth-sample.dat'



def main(argv):

  flow = client.flow_from_clientsecrets("client_secrets.json", scope=OAUTH_SCOPES)
  storage = Storage(CREDENTIAL_STORE_FILE)
  credentials = storage.get()
  http = credentials.authorize(httplib2.Http())

  # Retrieve command line arguments.
  # flags = dfareporting_utils.get_arguments(argv, __doc__, parents=[])

  # Authenticate and construct service.
  # service = dfareporting_utils.setup(flags)
  service = discovery.build('dfareporting', 'v3.1', http=http)

  try:
    # Construct the request.
    request = service.userProfiles().list()

    # Execute request and print response.
    response = request.execute()

    for profile in response['items']:
      print ('Found user profile with ID %s and name "%s" for account %s.'
             % (profile['profileId'], profile['userName'],
                profile['accountId']))

  except client.AccessTokenRefreshError:
    print ('The credentials have been revoked or expired, please re-run the '
           'application to re-authorize')


if __name__ == '__main__':
  main(sys.argv)
