from configparser import ConfigParser
import json
import boto3
import athena
import buckets
import kinesis
import emr
import vpc
import airflow
from builtins import open
import os

config = ConfigParser()
conf = json.load(open("src/config.conf"))

session = boto3.Session(profile_name='default')

bucket_names = conf.get("variables_buckets").get("bucket_names")
prefix = conf.get("variables_buckets").get("prefix")
bucket_dag = f'{conf.get("variables_buckets").get("bucket_dag_name")}'
bucket_dag_prefix = f'{prefix}-{bucket_dag}'
evn_name = conf.get("variables_airflow").get("evn_mwaa_name")
vpc_name = conf.get("variables_vpc").get("vpc_name")
path_local = conf.get("variables_buckets").get("path_src_local_files")
kinesis_stream_name = conf.get("variables_kinesis").get("kinesis_stream_name")
kinesis_delivery_stream_name = conf.get("variables_kinesis").get("kinesis_delivery_stream_name")
s3_output_staging_zone = conf.get("variables_kinesis").get("s3_output_staging_zone")
key_s3_bucket_staging_contracts = conf.get("variables_kinesis").get("key_s3_bucket_staging_contracts")
column_partition_output_staging_zone = conf.get("variables_kinesis").get("column_partition_output_staging_zone")
emr_stream_name = conf.get("variables_stream_emr").get("emr_stream_name")
s3_logs_output = conf.get("variables_stream_emr").get("s3_logs_output")
concurrent_steps = conf.get("variables_stream_emr").get("concurrent_steps")
DDL_results = conf.get("variables_stream_athena").get("DDL_results")
OutputLocation = conf.get("variables_stream_athena").get("OutputLocation")


def buckets_create_update():
    print("**********************************************************\n"
          "*                      Buckets                           *\n"
          "**********************************************************")
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
    print("**********************************************************\n"
          "*                      VPCs                              *\n"
          "**********************************************************")
    path_template_vpc_cloudformation = conf.get("variables_vpc").get("path_template_vpc_cloudformation")
    capabilities = conf.get("variables_vpc").get("capabilities")
    vpc.create_stack_template_vpc(vpc_name, path_template_vpc_cloudformation, capabilities, session,
                                  conf.get("variables_vpc"))


def create_apache_airflow():
    print("**********************************************************\n"
          "*                   Amazon MWAA                          *\n"
          "**********************************************************")
    vpc_ids = vpc.get_vpc_id(vpc_name, session, conf.get("variables_vpc").get("vpcCIDR"))
    subnets_id = vpc.get_private_subnets_id(vpc_ids, session)
    sec_group = vpc.get_security_group_id(vpc_ids, session)
    emr.create_roles_default_emr(session)
    airflow.create_rol_execution_evn(session, bucket_dag_prefix, evn_name)
    airflow.create_policy_emr_mwaa(session, evn_name)
    airflow.create_policy_athena_mwaa(session, evn_name)
    airflow.create_mwaa_evn(evn_name, bucket_dag_prefix, session, sec_group, subnets_id[1:])


def create_streams_flow():
    print("**********************************************************\n"
          "*                Stream Data Flow                        *\n"
          "**********************************************************")

    kinesis.create_role_kinesis(session, kinesis_delivery_stream_name)
    kinesis.create_stream_kinesis(session, kinesis_stream_name)
    kinesis.create_delivery_stream_kinesis(session, kinesis_delivery_stream_name, kinesis_stream_name,
                                           s3_output_staging_zone, key_s3_bucket_staging_contracts,
                                           column_partition_output_staging_zone)

    vpc_ids = vpc.get_vpc_id(vpc_name, session, conf.get("variables_vpc").get("vpcCIDR"))
    subnets_id = vpc.get_private_subnets_id(vpc_ids, session)
    print("tester", subnets_id[0])
    emr.run_job_flow_emr(session, emr_stream_name, concurrent_steps, s3_logs_output, subnets_id[0])
    athena.query_execution(session, DDL_results, OutputLocation)


if __name__ == '__main__':
    buckets_create_update()
    create_vpc_subnets()
    create_apache_airflow()
    create_streams_flow()