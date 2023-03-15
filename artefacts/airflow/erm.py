import logging
import time

from botocore.exceptions import ClientError

import iam


def create_roles_default_erm(session_client, evn_name):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param evn_name:
    :param session_client:
    :return: True if bucket created, else False
    """
    try:
        identity = session_client.client('sts').get_caller_identity()

        policy_name = f'PassRole_EMR_EC2-policy'
        policy_name1 = "AmazonEMRServicePolicy_v2"
        policy_name2 = "AmazonElasticMapReduceRole_v2"
        role_name = f'{evn_name}-role'
        role_name1 = f'EMR_DefaultRole'
        role_name2 = f'EMR_EC2_DefaultRole'
        role_name3 = f'EMR_DefaultRole_V2'

        """
        iam.create_role(session_client, role_name1,
                        open("artefacts/airflow/emr_assume_default_policy_doc.json").read())
        iam.attach_role_policy(session_client, role_name1,
                               'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole')
        """
        file = open("artefacts/airflow/emr_assume_default_policy_doc_V2.json", "r")
        ct_file = file.read().replace("{your-account-id}", identity.get("Account"))\
            .replace("{your-region}", session_client.region_name)
        file.close()
        iam.create_policy(session_client, policy_name1, open("artefacts/airflow/emr_policy_doc_V2.json").read())
        iam.create_role(session_client, role_name3, ct_file)
        iam.attach_role_policy(session_client, role_name3, f'arn:aws:iam::{identity.get("Account")}:policy/{policy_name1}')
        time.sleep(5)

        iam.create_policy(session_client, policy_name2, open("artefacts/airflow/emr_ec2_policy_doc.json").read())
        iam.create_role(session_client, role_name2,
                        open("artefacts/airflow/emr_ec2_assume_default_policy_doc.json").read())
        iam.attach_role_policy(session_client, role_name2, f'arn:aws:iam::{identity.get("Account")}:policy/{policy_name2}')
        time.sleep(5)

        file = open("artefacts/airflow/mwaa_emr_ec2_policy_doc.json", "r")
        ct_file = file.read().replace("{your-account-id}", identity.get("Account"))
        file.close()
        iam.create_policy(session_client, policy_name, ct_file)
        iam.attach_role_policy(session_client, role_name, f'arn:aws:iam::{identity.get("Account")}:policy/{policy_name}')

        print("Creating role execution evn...")
        time.sleep(25)
    except ClientError as e:
        logging.error(e)
        return False
    print("Created role execution evn success")
    return True

def deleted_roles_default_erm(session_client):
    """Create a role execution environment for MWAA

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param evn_name:
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
        iam.delete_role(session_client, f'EMR_EC2_DefaultRole')
        time.sleep(5)

    except ClientError as e:
        logging.error(e)
        return False
    print("deleted role default erm")
    return True
