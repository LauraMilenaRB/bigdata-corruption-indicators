"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import logging
import time
from botocore.exceptions import ClientError
import iam


def create_rol_execution_evn(session_client, bucked_dags_name, evn_name):
    """Creación del rol del entorno de ejecución para Amazon MWAA

    @param evn_name: Nombre del entorno del servicio MWAA
    @param session_client: Sesión AWS
    @param bucked_dags_name: Nombre del bucket donde está el DAG
    @return: True si el rol del entorno de ejecución se crea, si no False
    """
    try:
        identity = session_client.client('sts').get_caller_identity()

        file = open("aws-services/airflow/executionevn_policy_doc.json", "r")
        ct_file = file.read() \
            .replace("{your-s3-bucket-name}", bucked_dags_name) \
            .replace("{your-environment-name}", evn_name) \
            .replace("{your-account-id}", identity.get("Account")) \
            .replace("{your-region}", session_client.region_name)
        file.close()
        policy_name = f'{evn_name}-policy'
        role_name = f'{evn_name}-role'

        iam.create_policy(session_client, policy_name, ct_file)
        iam.create_role(session_client, role_name,
                        open("aws-services/airflow/executionevn_assume_policy_doc.json").read())
        iam.attach_role_policy(session_client, role_name,
                               f'arn:aws:iam::{identity.get("Account")}:policy/{policy_name}')
        iam.attach_role_policy(session_client, role_name, 'arn:aws:iam::aws:policy/AmazonS3FullAccess')
        iam.attach_role_policy(session_client, role_name, 'arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccess')
    except ClientError as e:
        logging.error(e)
        return False
    else:
        time.sleep(20)
        print(f"Creado el rol ejecución para el entorno MWAA {evn_name} con éxito")
        return True


def create_policy_emr_mwaa(session_client, evn_name):
    """Creación de la política Amazon EMR para el entorno de ejecución MWAA

    @param evn_name: Nombre del entorno del servicio MWAA
    @param session_client: Sesión AWS
    @return: True si la política del servicio emr del entorno mwaa se crea, si no False
    """
    try:
        identity = session_client.client('sts').get_caller_identity()

        policy_name = f'PassRole_EMR_EC2-policy'
        role_name = f'{evn_name}-role'

        file = open("aws-services/airflow/mwaa_emr_ec2_policy_doc.json", "r")
        ct_file = file.read().replace("{your-account-id}", identity.get("Account"))
        file.close()
        iam.create_policy(session_client, policy_name, ct_file)
        iam.attach_role_policy(session_client, role_name,
                               f'arn:aws:iam::{identity.get("Account")}:policy/{policy_name}')

    except ClientError as e:
        logging.error(e)
        return False
    else:
        time.sleep(20)
        print(f"Creada la política {policy_name} para el entorno de ejecución MWAA {evn_name} con éxito")
        return True


def create_policy_redshift_mwaa(session_client, evn_name):
    """Creación de la política Amazon Redshift para el entorno de ejecución MWAA

    @param evn_name: Nombre del entorno del servicio MWAA
    @param session_client: Sesión AWS
    @return: True si la política del servicio redshift del entorno mwaa se crea, si no False
    """
    try:
        identity = session_client.client('sts').get_caller_identity()

        policy_name = f'{evn_name}-Redshift-policy'
        role_name = f'{evn_name}-role'

        file = open("aws-services/redshift/redshift_assume_policy_doc.json", "r")
        ct_file = file.read().replace("{your-account-id}", identity.get("Account"))
        file.close()
        iam.create_policy(session_client, policy_name, ct_file)
        iam.attach_role_policy(session_client, role_name,
                               f'arn:aws:iam::{identity.get("Account")}:policy/{policy_name}')

    except ClientError as e:
        logging.error(e)
        return False
    else:
        time.sleep(20)
        print(f"Creada la política {policy_name} para el entorno de ejecución MWAA {evn_name} con éxito")
        return True


def create_mwaa_evn(evn_name, bucked_dags_name, session_client, security_ids, subnet_ids):
    """Creación entornó Amazon MWAA

    @param subnet_ids: Ids subredes de la vpc
    @param security_ids: Ids grupos de seguridad de la vpc
    @param evn_name: Nombre del entorno del servicio MWAA
    @param bucked_dags_name: Nombre del bucket donde está el DAG
    @param session_client: Sesión AWS
    @return: True si el entorno se crea, si no False
    """
    try:
        role_exc_evn = session_client.client('iam').get_role(
            RoleName=f'{evn_name}-role'
        )
        session_client.client('mwaa').create_environment(
            AirflowVersion='2.4.3',
            DagS3Path='dags/',
            EnvironmentClass='mw1.small',
            ExecutionRoleArn=role_exc_evn.get("Role").get("Arn"),
            LoggingConfiguration={
                'DagProcessingLogs': {
                    'Enabled': True,
                    'LogLevel': 'INFO'
                },
                'SchedulerLogs': {
                    'Enabled': True,
                    'LogLevel': 'INFO'
                },
                'TaskLogs': {
                    'Enabled': True,
                    'LogLevel': 'INFO'
                },
                'WebserverLogs': {
                    'Enabled': True,
                    'LogLevel': 'INFO'
                },
                'WorkerLogs': {
                    'Enabled': True,
                    'LogLevel': 'INFO'
                }
            },
            MaxWorkers=1,
            MinWorkers=1,
            Name=evn_name,
            NetworkConfiguration={
                'SecurityGroupIds': security_ids,
                'SubnetIds': subnet_ids
            },
            SourceBucketArn=f'arn:aws:s3:::{bucked_dags_name}',
            WebserverAccessMode='PUBLIC_ONLY'
        )

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print(f"Creando el entorno Amazon MWAA {evn_name}...")
        time.sleep(60)
        print(f"Creado el entorno MWAA {evn_name} con éxito")
        return True


def deleted_mwaa_evn(evn_name, session_client):
    """Eliminación del entorno de Amazon de MWAA

    @param evn_name: Nombre del entorno del servicio MWAA
    @param session_client: Sesión AWS
    @return: True si el entorno mwaa se elimina, si no False
    """
    try:
        session_client.client('mwaa').delete_environment(
            Name=evn_name
        )
    except ClientError as e:
        logging.error(e)
        return False
    else:
        time.sleep(60)
        print(f"Eliminado el entorno de Amazon MWAA {evn_name} con éxito")
        return True


def deleted_rol_execution_evn(session_client, evn_name):
    """Eliminación del rol para el entorno de Amazon de MWAA

    @param evn_name:  Nombre del entorno del servicio MWAA
    @param session_client: Sesión AWS
    @return: True si el rol del entorno mwaa se elimina, si no False
    """
    try:
        iam.detach_role_policy(session_client, f'{evn_name}-policy', f'{evn_name}-role')
        iam.detach_role_policy(session_client, f'PassRole_EMR_EC2-policy', f'{evn_name}-role')
        iam.detach_role_policy_aws(session_client, f'AmazonS3FullAccess', f'{evn_name}-role')
        iam.detach_role_policy_aws(session_client, f'AmazonRedshiftAllCommandsFullAccess', f'{evn_name}-role')
        iam.delete_role(session_client, f'{evn_name}-role')

    except ClientError as e:
        logging.error(e)
        return False
    else:
        time.sleep(20)
        print(f"Eliminado el rol del entorno de Amazon MWAA {evn_name} con éxito")
        return True
