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
bucket_dag = f'{conf.get("variables_buckets").get("bucket_dag_name")}'
bucket_dag_prefix = f'{prefix}-{bucket_dag}'
evn_name = conf.get("variables_airflow").get("evn_mwaa_name")
vpc_name = conf.get("variables_vpc").get("vpc_name")
path_local = conf.get("variables_buckets").get("path_src_local_files")


def buckets_create_update():
    for name in bucket_names:
        b_name = f'{prefix}-{name}'
        buckets.create_bucket(b_name, None, session)
    buckets.put_public_access_block(bucket_dag_prefix, session)

    path_local_files = os.listdir(f"{path_local}")

    for directory_bucket in path_local_files:
        if directory_bucket != "config.conf":
            path_local_bucket = f"{path_local}{directory_bucket}"
            for directory_bucket_key in os.listdir(f"{path_local_bucket}"):
                path_local_bucket2 = f"{path_local_bucket}/{directory_bucket_key}"
                other_file_name = os.listdir(f"{path_local_bucket2}")
                for file_name in other_file_name:
                    buckets.upload_file(f"{path_local_bucket2}/{file_name}", f'{prefix}-{directory_bucket}',
                                        f"{directory_bucket_key}/{file_name}", session)
                    print(f"Upload susses file {directory_bucket_key}/{file_name}")
    print("Upload susses files")


def create_vpc_subnets():
    path_template_vpc_cloudformation = conf.get("variables_vpc").get("path_template_vpc_cloudformation")
    capabilities = conf.get("variables_vpc").get("capabilities")
    vpc.create_stack_template_vpc(vpc_name, path_template_vpc_cloudformation, capabilities, session,
                                  conf.get("variables_vpc"))


def create_apache_airflow():
    vpc_ids = vpc.get_vpc_id(vpc_name, session, conf.get("variables_vpc").get("vpcCIDR"))
    subnets_id = vpc.get_private_subnets_id(vpc_ids, session)
    sec_group = vpc.get_security_group_id(vpc_ids, session)
    airflow.create_mwaa_evn(evn_name, bucket_dag_prefix, session, sec_group, subnets_id[1:])
    erm.create_roles_default_erm(session, evn_name)


if __name__ == '__main__':
    #buckets_create_update()
    create_vpc_subnets()
    #create_apache_airflow()
    # deleted_buckets()
