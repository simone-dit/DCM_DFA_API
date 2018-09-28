import gs_add_remove

import _test_get_prisma_report
import gsql_upload

import subprocess


# PRISMA


_test_get_prisma_report.get_prisma('DIAGEO')
print()
# from _test_get_prisma_report import file_name
fileName_prisma = 'Monthly_PRISMA_F19.csv'

prisma_trim_cmd = 'sed -i 1d {}'.format(fileName_prisma)
subprocess.call(prisma_trim_cmd.split(), shell=True)

gs_add_remove.gs_add(fileName_prisma, "diageo_prisma")
print('%s uploaded to bucket "gs://neallab/diageo_prisma/".' % fileName_prisma)
print()



gsql_upload.gsql_imp("diageo_prisma", fileName_prisma, "prisma")
