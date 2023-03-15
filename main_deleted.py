import time
from configparser import ConfigParser
import json
import boto3

import buckets
import erm
import iam
import vpc
import airflow
from builtins import open
import os

config = ConfigParser()
conf = json.load(open("src/config.conf"))

session = boto3.Session(profile_name='default')

bucket_names = conf.get("variables_buckets").get("bucket_names")
prefix = conf.get("variables_buckets").get("prefix")
bucket_dag = f'{prefix}-{conf.get("variables_buckets").get("bucket_dag_name")}'
evn_name = conf.get("variables_airflow").get("evn_mwaa_name")
vpc_name = conf.get("variables_vpc").get("vpc_name")


def deleted_buckets():
    for name in bucket_names:
        b_name = f'{prefix}-{name}'
        os.system(f'aws s3 rm s3://{b_name} --recursive')
        buckets.deleted_buckets(session, b_name)
        time.sleep(3)
    print("Deleted susses all buckets")


def deleted_vpc():
    vpc.deleted_stack_template_vpc(session, vpc_name)


def deleted_role():
    airflow.deleted_rol_execution_evn(session, evn_name)
    erm.deleted_roles_default_erm(session)



def deleted_airflow():
    airflow.deleted_mwaa_evn(evn_name, session)


if __name__ == '__main__':
    #deleted_buckets()
    #deleted_vpc()
    deleted_role()
    #deleted_airflow()
