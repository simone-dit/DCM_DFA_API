import os


def gs_add(filename, folder):
    os.chdir("C:/Users/zshuai01/PycharmProjects/DCM_PRISMA_connect")
    os.system("gsutil cp {} gs://neallab/{}/".format(filename, folder))


def gs_rm(filename, folder):
    os.system("gsutil rm gs://neallab/catalyst_campaign/{}".format(filename))

