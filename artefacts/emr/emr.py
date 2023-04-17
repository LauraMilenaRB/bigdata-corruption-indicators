import logging
import time

from botocore.exceptions import ClientError

import iam

import subprocess


def create_roles_default_emr(session_client):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :return: True if bucket created, else False
    """
    try:
        subprocess.run("aws emr create-default-roles", shell=True)
        subprocess.run("aws iam create-service-linked-role --aws-service-name elasticmapreduce.amazonaws.com",
                       shell=True)

        iam.add_role_from_instance_profile(session_client, 'EMR_EC2_DefaultRole', 'EMR_EC2_DefaultRole')

    except ClientError as e:
        logging.error(e)
        return False
    print("Creating role default emr...")
    time.sleep(20)
    print("Created role default emr success")
    return True


def deleted_roles_default_emr(session_client):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param session_client:
    :return: True if bucket created, else False
    """
    try:
        iam.detach_role_policy_aws(session_client, f'service-role/AmazonElasticMapReduceRole', 'EMR_DefaultRole')
        time.sleep(5)
        iam.delete_role(session_client, f'EMR_DefaultRole')
        time.sleep(5)
        iam.detach_role_policy_aws(session_client, f'service-role/AmazonElasticMapReduceforEC2Role', 'EMR_EC2_DefaultRole')
        time.sleep(5)
        iam.remove_role_from_instance_profile(session_client, 'EMR_EC2_DefaultRole', 'EMR_EC2_DefaultRole')
        iam.delete_role(session_client, f'EMR_EC2_DefaultRole')
        time.sleep(5)
        iam.detach_role_policy_aws(session_client, f'service-role/AmazonElasticMapReduceforAutoScalingRole', 'EMR_AutoScaling_DefaultRole')
        time.sleep(5)
        iam.delete_role(session_client, f'EMR_AutoScaling_DefaultRole')

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("deleted role default erm")
        return True


def run_job_flow_emr(session_client, emr_stream_name, concurrent_step, s3_logs_output, private_subnet_id,
                     endpoint, password, user, database):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param emr_stream_name:
    :param session_client:
    :param concurrent_step:
    :param s3_logs_output:
    :param private_subnet_id:
    :param endpoint:
    :param password:
    :param user:
    :param database:
    :return: True if bucket created, else False
    """
    try:
        client = session_client.client('emr')
        response = client.run_job_flow(
            Name=emr_stream_name,
            ReleaseLabel='emr-5.36.0',
            LogUri=f's3://{s3_logs_output}/elasticmapreduce/streaming/',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'MASTER',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm6g.xlarge',
                        'InstanceCount': 1
                    },
                    {
                        'Name': 'Core node',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm6g.xlarge',
                        'InstanceCount': 2
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': private_subnet_id,
            },
            Steps=[
                {
                    'Name': 'sudo install psycopg2',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'pip3', 'install', 'psycopg2-binary']

                    }
                },
                {
                    'Name': 'spark_stream_etl',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', '--master', 'yarn',
                                 '--deploy-mode', 'client', f's3://test-pgr-req-files/scripts/spark_stream_etl.py'
                                 ]

                    }
                },
                {
                    'Name': 'spark_stream_ind',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', '--master', 'yarn',
                                 '--deploy-mode', 'client', f's3://test-pgr-req-files/scripts/spark_stream_ind.py',
                                 '--endpoint', endpoint, '--pwd', password, '--user', user, '--db', database
                                 ]
                    }
                }
            ],
            Applications=[
                {
                    'Name': 'Spark'
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            AutoScalingRole='EMR_AutoScaling_DefaultRole',
            ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            StepConcurrencyLevel=int(concurrent_step)
        )

    except ClientError as e:
        logging.error(e)
        return False
    else:
        time.sleep(20)
        print("Run EMR job flow")
        return True
