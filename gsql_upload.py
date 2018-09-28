import subprocess


def gsql_imp(filePath, fileName, tableName):
    cloud_cmd = "echo Y | gcloud sql import csv nealanalytics gs://neallab/{}/{} --database=diageo --table={}"\
        .format(filePath, fileName, tableName)
    print(cloud_cmd)
    subprocess.call(cloud_cmd.split(), shell=True)