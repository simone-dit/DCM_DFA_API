# AUTHENTICATION using User Acct
# exe(open("authenticate_using_user_account.py").read())

# Get Profile_ID for acct_id = 6543
import sys
import argparse
import os
import io
import pandas
import datetime
from datetime import datetime, timedelta

import dfareporting_utils
from oauth2client import client
from googleapiclient import http

# Declare command-line flags
argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument(
    'report_name', type=str,
    help='The name of the report to download')

# Chunk size to use when downloading report files. Defaults to 32MB.
CHUNK_SIZE = 32 * 1024 * 1024


def main(argv):
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

        print('1 - Searching reports for Account ID 6543 (Diageo DCM) with Profile ID %s.'
              % (profile_id))
        print()

        #############

        # Construct the request.
        # Report ID
        request = service.reports().list(profileId=profile_id)

        while True:
            # Execute request and print response.
            response = request.execute()

            # get_report = [report for report in response['items'] if report['name'] == report_name]
            # print(get_report[0]['id'])

            for report in response['items']:
                if report['name'] == report_name:
                    print('2 - Found %s report with ID %s and name "%s".'
                          % (report['type'], report['id'], report['name']))
                    print()
                    report_id = report['id']

            if response['items'] and response['nextPageToken']:
                request = service.reports().list_next(request, response)
            else:
                break

        ##############

        # Construct a get request for the spsecified report.
        # File ID
        request = service.reports().files().list(
            profileId=profile_id, reportId=report_id)

        while True:
            # Execute request and print response.
            response = request.execute()

            current_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
            print('3 - Searching report files with end date %s.'
                  % (current_date))
            print()

            for report_file in response['items']:
                if report_file['dateRange']['endDate'] == current_date:
                    print('4 - Found report file with ID %s and file name "%s" with end date %s.'
                          % (report_file['id'], report_file['fileName'],
                             report_file['dateRange']['endDate']))
                    print()
                    file_id = report_file['id']
                    break  # BREAK: ensures the latest one got picked
                else:
                    print('UNABLE TO FIND FILE. Query request terminated.')
                    exit()

            if response['items'] and response['nextPageToken']:
                request = service.reports().files().list_next(request, response)
            else:
                break

        ################

        # Construct a request to download report file
        # Retrieve the file metadata
        print('5 - Downloading file with ID %s' % (file_id))

        report_file = service.files().get(reportId=report_id,
                                          fileId=file_id).execute()

        if report_file['status'] == 'REPORT_AVAILABLE':
            # Prepare a local file to download the report contents to.
            out_file = io.FileIO(generate_file_name(report_file), mode='wb')

            # Create a get request.
            request = service.files().get_media(reportId=report_id, fileId=file_id)

            # Create a media downloader instance.
            # Optional: adjust the chunk size used when downloading the file.
            downloader = http.MediaIoBaseDownload(out_file, request, chunksize=CHUNK_SIZE)

            # Execute the get request and download the file.
            download_finished = False
            while download_finished is False:
                _, download_finished = downloader.next_chunk()

            print('File %s downloaded to %s'
                  % (report_file['id'], os.path.realpath(out_file.name)))

    #############

    except client.AccessTokenRefreshError:
        print('The credentials have been revoked or expired, please re-run the '
              'application to re-authorize')


def generate_file_name(report_file):
    """Generates a report file name based on the file metadata."""
    # If no filename is specified, use the file ID instead.
    file_name = report_file['fileName'] + '_' + pandas.to_datetime('today').strftime('%Y-%m-%d') or report_file['id']
    extension = '.csv' if report_file['format'] == 'CSV' else '.xml'
    return file_name + extension


if __name__ == '__main__':
    main(sys.argv)
