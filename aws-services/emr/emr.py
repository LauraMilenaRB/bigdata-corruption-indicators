"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import logging
import time
from botocore.exceptions import ClientError
import iam
import subprocess


def create_roles_default_emr(session_client):
    """Creación de roles por default de Amazon EMR

    @param session_client: Sesión AWS
    @return: True si, else False
    """
    try:
        file = open("aws-services/emr/EMR_policy_AddJobStep.json", "r")
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
    else:
        print("Creando los roles default de Amazon EMR...")
        time.sleep(20)
        print("Creado los roles default de Amazon EMR con éxito")
        return True


def deleted_roles_default_emr(session_client):
    """Creación de los roles por default de Amazon EMR

    @param session_client: Sesión AWS
    @return: True si, si no False
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
        print("Eliminados los roles por default de Amazon EMR con éxito")
        return True


def deleted_job_flow_emr(session_client, id_cluster):
    """Eliminación del servicio Amazon EMR

    @param session_client: Sesión AWS
    @param id_cluster: object
    @return: True si ..., si no False

    """
    try:
        emr_client = session_client.client('emr')
        if id_cluster != 0:
            emr_client.terminate_job_flows(JobFlowIds=[id_cluster])

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print(f"Eliminando servicio Amazon EMR {id_cluster}...")
        time.sleep(20)
        print(f"Eliminado servicio Amazon EMR {id_cluster} con éxito")
        return True


def get_id_job_flow_emr(session_client, cluster_name):
    """Obtener id del servicio Amazon EMR
    
    @param session_client: Sesión AWS
    @param cluster_name:
    @return: True si ..., si no False
    """
    try:
        emr = session_client.client('emr')
        clusters = emr.list_clusters()
        your_cluster = [i for i in clusters['Clusters'] if i['Name'] == cluster_name][0]
        cluster = emr.describe_cluster(ClusterId=your_cluster['Id'])['Cluster']
        response = 0
        if cluster['Status']['State'] != 'TERMINATED':
            response = cluster['Id']

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print(f"Obtención id {response} con éxito")
        return response


def add_job_flow_steps(session_client, id_cluster, endpoint, password, user, database):
    """Agregar steps al servivio de Amazon EMR

    @param session_client: Sesión AWS
    @param  database:
    @param  user:
    @param  password:
    @param  endpoint:
    @param  id_cluster:
    @return: True si el step se agrega, si no False
    """
    try:
        client = session_client.client('emr')
        response = client.add_job_flow_steps(
            JobFlowId=id_cluster,
            Steps=[
                {
                    'Name': 'spark_stream_etl',
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
    else:
        print("Step agregado con éxito")
        return response


def run_job_flow_emr(session_client, emr_stream_name, concurrent_step, s3_logs_output, private_subnet_id):
    """Ejecución del servicio Amazon EMR

    @param session_client: Sesión AWS
    @param emr_stream_name:
    @param concurrent_step:
    @param s3_logs_output:
    @param private_subnet_id:
    @return: True si ..., si no False
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
        print("Ejecutado el servicio Amazon EMR con éxito")
        return True
