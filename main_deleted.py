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
region = conf.get("providers_aws").get("region")
session = boto3.Session(
    aws_access_key_id=conf.get("providers_aws").get("ACCESS_KEY"),
    aws_secret_access_key=conf.get("providers_aws").get("SECRET_KEY"),
)

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
    iam.detach_role_policy(session, f'{evn_name}-policy', f'{evn_name}-role')
    iam.detach_role_policy_aws(session, f'AmazonS3FullAccess', f'{evn_name}-role')
    iam.delete_role_all_dep(session, f'{evn_name}-policy', f'{evn_name}-role')


if __name__ == '__main__':
    deleted_buckets()
    #deleted_vpc()
    #deleted_role()
