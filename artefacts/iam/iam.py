import logging
import time

from botocore.exceptions import ClientError


def attach_role_policy(session_client, role_name, arn_policy, path=None):
    """Put block public access to bucket S3

    :param path:
    :param arn_policy:
    :param role_name:
    :param session_client:
    :return: True if file was uploaded, else False
    """
    try:
        iam = session_client.client('iam')
        response = iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=arn_policy
        )
        print(f"Creating role policy {role_name} {arn_policy}")
        time.sleep(10)
    except ClientError as e:
        logging.error(e)
        return None
    return response


def create_policy(session_client, policy_name, policy, path=None):
    """Put block public access to bucket S3

    :param path:
    :param policy_name:
    :param session_client:
    :return: True if file was uploaded, else False
    """
    try:
        iam = session_client.client('iam')
        response = iam.create_policy(
            PolicyName=policy_name,
            PolicyDocument=policy,
        )
        print(f"Creating policy {policy_name}")
        time.sleep(10)
    except ClientError as e:
        logging.error(e)
        return None
    return response


def create_role(session_client, role_name, assume_policy=None, path=None):
    """Put block public access to bucket S3

    :param path:
    :param assume_policy:
    :param role_name:
    :param session_client:
    :return: True if file was uploaded, else False
    """
    try:
        iam = session_client.client('iam')
        if assume_policy is None:
            response = iam.create_role(
                RoleName=role_name
            )
        else:
            response = iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=assume_policy
            )
        print(f"Creating role {role_name}")
        time.sleep(10)
    except ClientError as e:
        logging.error(e)
        return None
    return response


def detach_role_policy(session_client, policy_name, role_name):
    """Put block public access to bucket S3

    :param role_name:
    :param policy_name:
    :param session_client:
    :return: True if file was uploaded, else False
    """
    try:
        sts_client = session_client.client('sts')
        iam_client = session_client.client('iam')

        account_id = sts_client.get_caller_identity()['Account']
        policy_arn = f'arn:aws:iam::{account_id}:policy/{policy_name}'

        iam_client.detach_role_policy(
            PolicyArn=policy_arn,
            RoleName=role_name
        )
        iam_client.delete_role_policy(
            RoleName=role_name,
            PolicyName=policy_name
        )
        print(f"Detach role policy {role_name}")
        time.sleep(10)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def detach_role_policy_aws(session_client, policy_name, role_name):
    """Put block public access to bucket S3

    :param role_name:
    :param policy_name:
    :param session_client:
    :return: True if file was uploaded, else False
    """
    try:
        sts_client = session_client.client('sts')
        iam_client = session_client.client('iam')

        policy_arn = f'arn:aws:iam::aws:policy/{policy_name}'

        iam_client.detach_role_policy(
            PolicyArn=policy_arn,
            RoleName=role_name
        )
        iam_client.delete_role_policy(
            RoleName=role_name,
            PolicyName=policy_name
        )
        print(f"Detach role policy {role_name}")
        time.sleep(10)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def delete_role_all_dep(session_client, policy_name, role_name):
    """Put block public access to bucket S3

    :param role_name:
    :param policy_name:
    :param session_client:
    :return: True if file was uploaded, else False
    """
    try:
        sts_client = session_client.client('sts')
        iam_client = session_client.client('iam')

        account_id = sts_client.get_caller_identity()['Account']
        policy_arn = f'arn:aws:iam::{account_id}:policy/{policy_name}'
        iam_client.delete_role(RoleName=role_name)
        iam_client.delete_policy(PolicyArn=policy_arn)
    except ClientError as e:
        logging.error(e)
        return False
    return True
