import gs_add_remove
import _test_get_dcm_report


import datetime
from datetime import datetime, timedelta

import subprocess


# IN: input DCM report name
# DO: run report and download to local
report_name = "test_dcm_daily"
_test_get_dcm_report.main(report_name)

# local processt_
current_date = datetime.strftime(datetime.now(), '%Y-%m-%d')
report_end_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
raw_file_dcm = report_name + '_'+ current_date + '_raw.csv'
clean_file_dcm = report_name +'_' + report_end_date + '.csv'

dcm_trim_cmd = 'sed 1,12d;$d {} > {}'.format(raw_file_dcm, clean_file_dcm)
subprocess.call(dcm_trim_cmd.split(), shell=True)

gs_add_remove.gs_add(clean_file_dcm, "diageo_dcm")
print('%s uploaded to bucket "gs://neallab/diageo_dcm/".' % clean_file_dcm)


