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
        role_name = f'{evn_name}-role'
        role_name1 = f'EMR_DefaultRole'
        role_name2 = f'EMR_EC2_DefaultRole'

        iam.create_role(session_client, role_name1,
                        open("artefacts/airflow/emr_assume_default_policy_doc.json").read())
        iam.attach_role_policy(session_client, role_name1, 'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole')

        iam.create_role(session_client, role_name1,
                        open("artefacts/airflow/emr_ec2_assume_default_policy_doc.json").read())
        iam.attach_role_policy(session_client, role_name2,
                               'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role')

        file = open("artefacts/airflow/mwaa_emr_ec2_policy_doc.json", "r")
        ct_file = file.read() \
            .replace("{your-account-id}", identity.get("Account"))
        file.close()

        iam.create_policy(session_client, policy_name, ct_file)
        iam.attach_role_policy(session_client, role_name, f'arn:aws:iam::{identity.get("Account")}:policy/{policy_name}')

        print("Creating...")
        time.sleep(25)
        print("Created role execution evn success")
    except ClientError as e:
        logging.error(e)