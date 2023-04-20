"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import logging
import time
import psycopg2
from botocore.exceptions import ClientError
import iam


def create_roles_default_redshift(session_client, redshift_name):
    """Creación rol default Redshift
    
    @param redshift_name: 
    @param session_client: object
    @return: True si ..., si no False
    """
    try:
        policy_name = f'{redshift_name}-policy'
        role_name = f'{redshift_name}-role'

        identity = session_client.client('sts').get_caller_identity()

        file = open("aws-services/redshift/redshift_policy_doc.json")

        iam.create_policy(session_client, policy_name, file.read())
        file.close()

        iam.create_role(session_client, role_name, open("aws-services/redshift/redshift_assume_policy_doc.json").read())
        iam.attach_role_policy(session_client, role_name, 'arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccess')
        iam.attach_role_policy(session_client, role_name,
                               f'arn:aws:iam::{identity.get("Account")}:policy/{policy_name}')

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Creando rol default redshift...")
        time.sleep(10)
        print("Creado rol default redshift con éxito")
        return True


def create_redshift_serverless(session_client, redshift_name):
    """Creación cluster Redshift Serverless

    @param redshift_name: 
    @param session_client:
    @return: True si ..., si no False
    """
    try:

        client = session_client.client('redshift-serverless')

        response = client.create_workgroup(
            enhancedVpcRouting=False,
            namespaceName=f"{redshift_name}-namespace",
            port=5439,
            publiclyAccessible=True,
            workgroupName=f"{redshift_name}-workgroup",
        )

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Creando serverless redshift...")
        time.sleep(130)
        print("Creado serverless redshift con éxito")
        return True


def create_redshift_cluster(session_client, redshift_name, password_db, username_db, name_bd):
    """Creación cluster Redshift
    
    @param name_bd: 
    @param username_db: 
    @param password_db: 
    @param redshift_name: 
    @param session_client:
    @return: True si ..., si no False
    """
    try:
        client = session_client.client('redshift')
        identity = session_client.client('sts').get_caller_identity()
        response = client.create_cluster(
            DBName=f"{name_bd}",
            ClusterIdentifier=f"{redshift_name}-cluster-1",
            ClusterType='single-node',
            NodeType='dc2.large',
            MasterUsername=username_db,
            MasterUserPassword=password_db,
            AllowVersionUpgrade=False,
            NumberOfNodes=1,
            Encrypted=False,
            Port=5439,
            IamRoles=[
                f'arn:aws:iam::{identity.get("Account")}:role/{redshift_name}-role',
            ]
        )

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Creando cluster redshift...")
        time.sleep(130)
        print("Creado cluster redshift con éxito")
        return response


def create_query_redshift(query, password_db, username_db, name_bd, end_point):
    """Ejecución query Redshift

    @param query:
    @param end_point:
    @param name_bd:
    @param username_db:
    @param password_db:
    @return: True si ..., si no False

    """
    try:
        conn = psycopg2.connect(
            host=end_point,
            port=5439,
            user=username_db,
            password=password_db,
            database=name_bd
        )

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        if 'select' in query:
            result: tuple = cursor.fetchall()
            print(result)
        conn.close()

    except Exception as e:
        logging.error(e)
        return False
    else:
        print("Ejecutando query redshift...")
        time.sleep(10)
        print("Ejecutada query redshift con éxito")
        return True


def deleted_cluster_redshift(session_client, redshift_name):
    """Eliminación cluster Amazon Redshift

    @param session_client:
    @param redshift_name:
    @return: True si ..., si no False
    """

    try:
        client = session_client.client('redshift')
        client.delete_cluster(
            ClusterIdentifier=f"{redshift_name}-cluster-1",
            SkipFinalClusterSnapshot=True
        )

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print(f"Eliminando cluster redshift {redshift_name}...")
        time.sleep(20)
        print(f"Eliminado cluster redshift {redshift_name} con éxito")
        return True


def deleted_roles_default_redshift(session_client, redshift_name):
    """Eliminación rol default Redshift

    @param session_client:
    @param redshift_name: object
    @return: True si ..., si no False
    """

    try:
        iam.detach_role_policy(session_client, f'{redshift_name}-policy', f'{redshift_name}-role')
        iam.detach_role_policy_aws(session_client, f'AmazonRedshiftAllCommandsFullAccess', f'{redshift_name}-role')
        iam.delete_role(session_client, f'{redshift_name}-role')

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print(f"Eliminando roles default redshift {redshift_name}...")
        time.sleep(20)
        print(f"Eliminado cluster redshift {redshift_name} con éxito")
        return True


def access_conf_query(session_client, cluster_list):
    """Configuración acceso a ejecución de query

    @param session_client:
    @param cluster_list:
    @return: True si ..., si no False
    """

    try:
        ec2 = session_client.resource('ec2')
        vpc = ec2.Vpc(id=cluster_list['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(5439),
            ToPort=int(5439)
        )

    except ClientError as e:
        logging.error(e)
        return False
    else:
        time.sleep(10)
        print("Acceso disponible a ejecución query redshift con éxito")
        return True
