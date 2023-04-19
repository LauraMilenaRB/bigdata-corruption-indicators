import logging
import time
import psycopg2
from botocore.exceptions import ClientError
import iam

import subprocess


def create_roles_default_redshift(session_client, redshift_name):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :return: True if bucket created, else False
    """
    try:
        policy_name = f'{redshift_name}-policy'
        role_name = f'{redshift_name}-role'

        identity = session_client.client('sts').get_caller_identity()

        file = open("artefacts/redshift/redshift_policy_doc.json", "r")

        iam.create_policy(session_client, policy_name, file.read())
        file.close()

        iam.create_role(session_client, role_name, open("artefacts/redshift/redshift_assume_policy_doc.json").read())
        iam.attach_role_policy(session_client, role_name, 'arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccess')
        iam.attach_role_policy(session_client, role_name,
                               f'arn:aws:iam::{identity.get("Account")}:policy/{policy_name}')

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Creating role default redshift...")
        time.sleep(10)
        print("Created role default redshift success")
        return True


def create_redshift_serverless(session_client, redshift_name):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :return: True if bucket created, else False
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
        print("Creating redshift...")
        time.sleep(10)
        print("Created redshift success")
        return True


def create_redshift_cluster(session_client, redshift_name, password_db, username_db, name_bd):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :return: True if bucket created, else False
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
        print("Creating redshift...")
        time.sleep(130)
        print("Created redshift success")
        return response


def create_query_redshift(query, password_db, username_db, name_bd, end_point):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :return: True if bucket created, else False
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
        print("Creating query redshift...")
        time.sleep(10)
        print("Created query redshift success")
        return True


def deleted_cluster_redshift(session_client, redshift_name):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :return: True if bucket created, else False
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
        print("Creating access query redshift...")
        time.sleep(10)
        print("Created access query redshift success")
        return True


def deleted_roles_default_redshift(session_client, redshift_name):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :return: True if bucket created, else False
    """

    try:
        iam.detach_role_policy(session_client, f'{redshift_name}-policy', f'{redshift_name}-role')
        iam.detach_role_policy_aws(session_client, f'AmazonRedshiftAllCommandsFullAccess', f'{redshift_name}-role')
        iam.delete_role(session_client, f'{redshift_name}-role')

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Creating access query redshift...")
        time.sleep(10)
        print("Created access query redshift success")
        return True


def access_conf_query(session_client, cluster_list):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :return: True if bucket created, else False
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
        print("Creating access query redshift...")
        time.sleep(10)
        print("Created access query redshift success")
        return True