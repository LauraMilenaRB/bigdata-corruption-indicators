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
        file = open("artefacts/emr/EMR_policy_AddJobStep.json", "r")
        ct_file = file.read()
        file.close()
        identity = session_client.client('sts').get_caller_identity()
        iam.create_policy(session_client, 'EMR_policy_AddJobStep', ct_file)
        iam.attach_role_policy(session_client, 'EMR_EC2_DefaultRole', f'arn:aws:iam::{identity.get("Account")}:policy/EMR_policy_AddJobStep')

        subprocess.run("aws emr create-default-roles", shell=True)
        subprocess.run("aws iam create-service-linked-role --aws-service-name elasticmapreduce.amazonaws.com", shell=True)

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
        iam.detach_role_policy_aws(session_client, f'EMR_policy_AddJobStep', 'EMR_EC2_DefaultRole')
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
        print("Deleted role default erm")
        return True


def deleted_job_flow_emr(session_client, id_cluster):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :return: True if bucket created, else False
    """
    try:
        emr_client = session_client.client('emr')
        emr_client.terminate_job_flows(JobFlowIds=[id_cluster])

    except ClientError as e:
        logging.error(e)
        return False
    print(f"Deleting emr job {id_cluster}...")
    time.sleep(20)
    print(f"Deleted emr job {id_cluster} success")
    return True


def get_id_job_flow_emr(session_client, cluster_name):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :return: True if bucket created, else False
    """
    try:
        clusters = session_client.list_clusters()
        your_cluster = [i for i in clusters['Clusters'] if i['Name'] == cluster_name][0]
        response = session_client.describe_cluster(ClusterId=your_cluster['Id'])
    except ClientError as e:
        logging.error(e)
        return False
    return response


def add_job_flow_steps(session_client, id_cluster, endpoint, password, user, database):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :return: True if bucket created, else False
    """
    try:
        client = session_client.client('emr')
        response = client.add_job_flow_steps(
            JobFlowId=id_cluster,
            Steps=[
                {
                    'Name': 'spark_stream_ind_mini_batch',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', '--master', 'yarn',
                                 '--deploy-mode', 'client', f's3://test-pgr-req-files/scripts/spark_stream_etl_mini_batch_loop.py',
                                 '--endpoint', endpoint, '--pwd', password, '--user', user, '--db', database, '--id_cluster', id_cluster
                                 ]
                    }
                }
            ]
        )
    except ClientError as e:
        logging.error(e)
        return False
    return response


def run_job_flow_emr(session_client, emr_stream_name, concurrent_step, s3_logs_output, private_subnet_id):
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
