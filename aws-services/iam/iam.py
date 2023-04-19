"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import logging
import time
from botocore.exceptions import ClientError


def attach_role_policy(session_client, role_name, arn_policy, path=None):
    """Atar política a rol

    @param path:
    @param arn_policy:
    @param role_name:
    @param session_client:
    @return: True if file was uploaded, else False
    """
    try:
        iam = session_client.client('iam')
        response = iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=arn_policy
        )

    except ClientError as e:
        logging.error(e)
        return None
    else:
        print(f"Atando política {arn_policy} a rol {role_name}...")
        time.sleep(10)
        print(f"Atada política {arn_policy} a rol {role_name} con éxito")
        return response


def create_policy(session_client, policy_name, policy, path=None):
    """Creación de política

    @param path:
    @param policy:
    @param policy_name:
    @param session_client:
    @return: True if file was uploaded, else False
    """
    try:
        iam = session_client.client('iam')
        response = iam.create_policy(
            PolicyName=policy_name,
            PolicyDocument=policy,
        )
    except ClientError as e:
        logging.error(e)
        return None
    else:
        time.sleep(5)
        print(f"Creada política {policy_name} con éxito")
        return response


def create_role(session_client, role_name, assume_policy=None, path=None):
    """Creación de rol

    @param path:
    @param assume_policy:
    @param role_name:
    @param session_client:
    @return: True if file was uploaded, else False
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

        time.sleep(10)
    except ClientError as e:
        logging.error(e)
        return None
    else:
        time.sleep(5)
        print(f"Creado rol {role_name} con éxito")
        return response


def detach_role_policy(session_client, policy_name, role_name):
    """Separar rol de política

    @param role_name:
    @param policy_name:
    @param session_client:
    @return: True if file was uploaded, else False
    """
    try:
        sts_client = session_client.client('sts')
        iam_client = session_client.client('iam')

        account_id = sts_client.get_caller_identity()['Account']
        policy_arn = f'arn:aws:iam::{account_id}:policy/{policy_name}'

        iam_client.detach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        iam_client.delete_policy(
            PolicyArn=policy_arn
        )

    except ClientError as e:
        logging.error(e)
        return False
    else:
        time.sleep(10)
        print(f"Separado rol {role_name} de política {policy_name} con éxito")
        return True


def detach_role_policy_aws(session_client, policy_name, role_name):
    """Separar rol de política AWS

    @param role_name:
    @param policy_name:
    @param session_client:
    @return: True if file was uploaded, else False
    """
    try:
        iam_client = session_client.client('iam')

        policy_arn = f'arn:aws:iam::aws:policy/{policy_name}'

        iam_client.detach_role_policy(
            PolicyArn=policy_arn,
            RoleName=role_name
        )

    except ClientError as e:
        logging.error(e)
        return False
    else:
        time.sleep(10)
        print(f"Separado rol {role_name} de política {policy_name} con éxito")
        return True


def delete_role(session_client, role_name):
    """Eliminación de rol

    @param role_name:
    @param session_client:
    @return: True if file was uploaded, else False
    """
    try:
        iam_client = session_client.client('iam')
        iam_client.delete_role(RoleName=role_name)

    except ClientError as e:
        logging.error(e)
        return False
    else:
        time.sleep(10)
        print(f"Eliminado rol {role_name}")
        return True


def remove_role_from_instance_profile(session_client, role_name, instance_profile):
    """Remover rol de instance profile

    @param role_name:
    @param session_client:
    @param instance_profile:
    @return: True if file was uploaded, else False
    """
    try:
        iam_client = session_client.client('iam')
        iam_client.remove_role_from_instance_profile(
            InstanceProfileName=instance_profile,
            RoleName=role_name
        )
    except ClientError as e:
        logging.error(e)
        return False
    else:
        time.sleep(10)
        print(f"Removido rol {role_name} de instance profile {instance_profile} con éxito")
        return True


def add_role_from_instance_profile(session_client, role_name, instance_profile):
    """Agregar rol a instance profile de AWS

    @param role_name:
    @param session_client:
    @param instance_profile:
    @return: True if file was uploaded, else False
    """
    try:
        iam = session_client.client('iam')
        response = iam.add_role_to_instance_profile(
            InstanceProfileName=instance_profile,
            RoleName=role_name
        )
    except ClientError as e:
        logging.error(e)
        return False
    else:
        time.sleep(10)
        print(f"Agregado rol {role_name} a instance profile {instance_profile} con éxito")
        return True
