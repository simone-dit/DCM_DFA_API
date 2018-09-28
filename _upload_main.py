import gs_add_remove
import _test_get_dcm_report
import _test_get_prisma_report
import gsql_upload
import os

import datetime
from datetime import datetime, timedelta

import subprocess
import platform



# IN: input DCM report name
# DO: run report and download to local
report_name = "API_Report_Diageo_US"
_test_get_dcm_report.main(report_name)

# local process
current_date = datetime.strftime(datetime.now(), '%Y-%m-%d')
report_end_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
raw_file_dcm = report_name + '_' + current_date + '_raw.csv'
clean_file_dcm = report_name + '_' + report_end_date + '.csv'

# 1. remove file header
dcm_trim_cmd = 'sed 1,12d;$d {} > {}'.format(raw_file_dcm, clean_file_dcm)
subprocess.call(dcm_trim_cmd.split(), shell=True)

# 2. upload: local drive to storage bucket
bucket_folder = "diageo_dcm"
gs_add_remove.gs_add(clean_file_dcm, bucket_folder)
print('"%s" uploaded from local drive %s to bucket "gs://neallab/%s/".'
      % (clean_file_dcm, os.getcwd(), bucket_folder))

# 3. remove local copy
if platform.system() == 'Windows':
    os.system("IF EXIST %s (rm %s) ELSE (ECHO file doesn't exist)" % (clean_file_dcm, clean_file_dcm))
elif platform.system() == 'Linux':
    os.system("[ -e %s ] && rm %s || echo file doesn't exist" % (clean_file_dcm, clean_file_dcm))



# 4. upload: storage bucket to MySQL database
gsql_upload.gsql_imp("diageo_dcm", clean_file_dcm, "dcm")
print('"%s" uploaded from bucket "gs://neallab/%s/" to MySQL table "diageo.dcm"'
      %(clean_file_dcm, bucket_folder))




# PRISMA


_test_get_prisma_report.get_prisma('DIAGEO')

# from _test_get_prisma_report import file_name
fileName_prisma = 'Monthly_PRISMA_F19.csv'

prisma_trim_cmd = 'sed -i 1d {}'.format(fileName_prisma)
subprocess.call(prisma_trim_cmd.split(), shell=True)

gs_add_remove.gs_add(fileName_prisma, "diageo_prisma")
print('%s uploaded to bucket "gs://neallab/diageo_prisma/".' % fileName_prisma)



gsql_upload.gsql_imp("diageo_prisma", fileName_prisma, "prisma")

