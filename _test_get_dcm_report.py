# AUTHENTICATION using User Acct
# exe(open("authenticate_using_user_account.py").read())

# Get Profile_ID for acct_id = 6543
import sys
import argparse
import os
import io
import pandas
import random
import time

import dfareporting_utils
from oauth2client import client
from googleapiclient import http

# Declare command-line flags
argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument(
    'report_name', type=str,
    help='The name of the report to download')

# from #
# run_report.py #

# The following values control retry behavior while the report is processing.
# Minimum amount of time between polling requests. Defaults to 10 seconds.
MIN_RETRY_INTERVAL = 10
# Maximum amount of time between polling requests. Defaults to 10 minutes.
MAX_RETRY_INTERVAL = 10 * 60
# Maximum amount of time to spend polling. Defaults to 1 hour.
MAX_RETRY_ELAPSED_TIME = 60 * 60

# from #
# download_file.py #
# Chunk size to use when downloading report files. Defaults to 32MB.
CHUNK_SIZE = 32 * 1024 * 1024


def main(argv):
    argv = [sys.argv, argv]

    # Retrieve command line arguments.
    flags = dfareporting_utils.get_arguments(argv, __doc__, parents=[argparser])

    # Authenticate and construct service.
    service = dfareporting_utils.setup(flags)

    report_name = flags.report_name

    ##############

    try:
        # Construct the request.
        # Profile ID
        request = service.userProfiles().list()

        # Execute request and print response.
        response = request.execute()

        get_profile = [profile for profile in response['items'] if profile['accountId'] in ['817771']]
        profile_id = get_profile[0]['profileId']

        print('1 - Searching reports for Account ID 817771 (Diageo DCM) with Profile ID %s.'
              % profile_id)
        print()

        #############

        # Construct the request.
        # Report ID
        request = service.reports().list(profileId=profile_id)

        while True:
            # Execute request and print response.
            response = request.execute()

            report_id = []
            for report in response['items']:
                if report['name'] == report_name:
                    print('2 - Found %s report with ID %s and name "%s".'
                          % (report['type'], report['id'], report['name']))
                    print()
                    report_id.append(report['id'])
                    break

            if response['items'] and response['nextPageToken']:
                request = service.reports().list_next(request, response)
            else:
                break

        ##############

        # Run report
        report_file = service.reports().run(profileId=profile_id,
                                            reportId=report_id[0]).execute()
        print('3. File with ID %s has been created' % report_file['id'])
        print()

        # Wait for the report file to finish processing.
        # An exponential backoff strategy is used to conserve request quota.
        sleep = 0
        start_time = time.time()
        while True:
            report_file = service.files().get(reportId=report_id[0],
                                              fileId=report_file['id']).execute()

            status = report_file['status']
            if status == 'REPORT_AVAILABLE':
                print('5. File status is %s, ready to download.' % status)
                ####
                # Prepare a local file to download the report contents to.
                print('hi')
                out_file = io.FileIO(generate_file_name(report_file), mode='wb')

                # Create a get request.
                request = service.files().get_media(reportId=report_id[0], fileId=report_file['id'])

                # Create a media downloader instance.
                # Optional: adjust the chunk size used when downloading the file.
                downloader = http.MediaIoBaseDownload(out_file, request,
                                                      chunksize=CHUNK_SIZE)

                # Execute the get request and download the file.
                download_finished = False
                while download_finished is False:
                    _, download_finished = downloader.next_chunk()

                print('File %s downloaded to %s'
                      % (report_file['id'], os.path.realpath(out_file.name)))
                ####
                return
            elif status != 'PROCESSING':
                print('5. File status is %s, processing failed.' % status)
                return
            elif time.time() - start_time > MAX_RETRY_ELAPSED_TIME:
                print('5. File processing deadline exceeded.')
                return

            sleep = next_sleep_interval(sleep)
            print('4. File status is %s, sleeping for %d seconds.' % (status, sleep))
            time.sleep(sleep)

    except client.AccessTokenRefreshError:
        print('The credentials have been revoked or expired, please re-run the '
              'application to re-authorize')


def next_sleep_interval(previous_sleep_interval):
    """Calculates the next sleep interval based on the previous."""
    min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
    max_interval = previous_sleep_interval * 3 or MIN_RETRY_INTERVAL
    return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))


def generate_file_name(report_file):
    """Generates a report file name based on the file metadata."""
    # If no filename is specified, use the file ID instead.
    file_name = report_file['fileName'] + '_' + pandas.to_datetime('today').strftime('%Y-%m-%d') + '_raw' or report_file['id']
    extension = '.csv' if report_file['format'] == 'CSV' else '.xml'
    return file_name + extension


if __name__ == '__main__':
    main(sys.argv)
