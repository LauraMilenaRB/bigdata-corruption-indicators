import time
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
bucket_dag = f'{prefix}-{conf.get("variables_buckets").get("bucket_dag_name")}'
evn_name = conf.get("variables_airflow").get("evn_mwaa_name")
vpc_name = conf.get("variables_vpc").get("vpc_name")
kinesis_stream_name = conf.get("variables_kinesis").get("kinesis_stream_name")
kinesis_delivery_stream_name = conf.get("variables_kinesis").get("kinesis_delivery_stream_name")
emr_stream_name = conf.get("variables_stream_emr").get("emr_stream_name")
name_table_results = conf.get("variables_stream_athena").get("name_table_results")
OutputLocation = conf.get("variables_stream_athena").get("OutputLocation")


def deleted_buckets():
    print("**********************************************************\n"
          "*                      Buckets                           *\n"
          "**********************************************************")
    for name in bucket_names:
        b_name = f'{prefix}-{name}'
        os.system(f'aws s3 rm s3://{b_name} --recursive')
        buckets.deleted_buckets(session, b_name)
        time.sleep(3)
    print("Deleted susses all buckets")


def deleted_vpc():
    print("**********************************************************\n"
          "*                        VPCs                            *\n"
          "**********************************************************")
    vpc.deleted_stack_template_vpc(session, vpc_name)


def deleted_airflow():
    print("**********************************************************\n"
          "*                    Amazon MWAA                         *\n"
          "**********************************************************")

    airflow.deleted_mwaa_evn(evn_name, session)
    airflow.deleted_rol_execution_evn(session, evn_name)
    emr.deleted_roles_default_emr(session)


def deleted_kinesis_stream():
    print("**********************************************************\n"
          "*                Stream Data Flow                        *\n"
          "**********************************************************")

    kinesis.delete_stream_kinesis(session, kinesis_stream_name)
    kinesis.delete_delivery_stream_kinesis(session, kinesis_delivery_stream_name)
    athena.query_execution(session, f"DROP TABLE IF EXISTS {name_table_results};", OutputLocation)
    kinesis.deleted_role_kinesis(session, kinesis_delivery_stream_name)


if __name__ == '__main__':
    deleted_buckets()
    deleted_vpc()
    deleted_airflow()
    deleted_kinesis_stream()

