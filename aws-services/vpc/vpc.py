"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import logging
import time
from botocore.exceptions import ClientError


def deleted_stack_template_vpc(session_client, stack_vpc_name):
    """Eliminación de Stack Cloudformation para vpc


    @param stack_vpc_name:
    @param session_client:
    @return: True si ..., si no False
    """
    try:
        cloudformation_client = session_client.client('cloudformation')
        cloudformation_client.delete_stack(
            StackName=stack_vpc_name
        )

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Eliminando stack Cloudformation para vpc...")
        time.sleep(15)
        print("Eliminado stack Cloudformation para vpc con éxito")
        return True


def created_default_vpc(session_client):
    """Creación de vpc default para Amazon EMR

    @param session_client:
    @return: True si ..., si no False
    """
    try:
        my_region = session_client.region_name
        session_client.client("ec2", region_name=my_region).create_default_vpc()
    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Creando VPC default...")
        time.sleep(30)
        print("Creada VPC default con éxito")
        return True


def deleted_default_vpc(session_client):
    """Eliminación de vpc default para Amazon EMR

    @param session_client:
    @return: True si ..., si no False
    """
    try:
        ec2_client = session_client.client("ec2")
        ec2_resource = session_client.resource("ec2")

        response = ec2_client.describe_vpcs(
            Filters=[{"Name": "is-default", "Values": ["true"]}]
        )

        vpc = ec2_resource.Vpc(response["Vpcs"][0]["VpcId"])
        vpc.load()

        for instance in vpc.instances.all():
            print(instance)

        for subnet in vpc.subnets.all():
            subnet.delete()

        for internet_gateway in vpc.internet_gateways.all():
            internet_gateway.detach_from_vpc(VpcId=vpc.id)
            internet_gateway.delete()

        for route_table in vpc.route_tables.all():
            print(route_table)

        for security_group in vpc.security_groups.all():
            print(security_group)

        for network_interface in vpc.network_interfaces.all():
            print(network_interface)

        vpc.delete()
    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Eliminando VPC default...")
        time.sleep(30)
        print("Eliminada VPC default con éxito")
        return True


def create_stack_template_vpc(stack_vpc_name, path_file, capabilities_par, session_client, conf_var):
    """Creación de Stack Cloudformation para vpc

    @param conf_var:
    @param stack_vpc_name:
    @param session_client:
    @param capabilities_par:
    @param path_file:
    @return: True si ..., si no False
    """
    try:
        cloudformation_client = session_client.client('cloudformation')

        file = open(path_file, "r")
        ct_file = file.read() \
            .replace("<vpc_name>", stack_vpc_name) \
            .replace("<vpcCIDR>", conf_var.get("vpcCIDR"))
        file.close()
        cloudformation_client.create_stack(
            StackName=stack_vpc_name,
            TemplateBody=ct_file,
            Capabilities=capabilities_par
        )

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Creando VPC en Cloudformation...")
        time.sleep(90)
        print("Creado VPC en Cloudformation con éxito")
        return True


def get_vpc_id(stack_vpc_name, session_client, vpcCIDR):
    """Obtener id de la vpc

    @param stack_vpc_name:
    @param session_client:
    @param vpcCIDR:
    @return: True si ..., si no False
    """
    try:
        client = session_client.client('ec2')
        response = client.describe_vpcs(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [
                        f'{stack_vpc_name}',
                    ]
                },
                {
                    'Name': 'cidr-block-association.cidr-block',
                    'Values': [vpcCIDR]
                },
            ]
        )
        resp = response['Vpcs']
        if len(resp) > 0:
            resp = resp[0].get("VpcId")
        else:
            print('No vpcs found')
    except ClientError as e:
        logging.error(e)
        return False
    else:
        return resp


def get_private_subnets_id(vpc_id, session_client):
    """Obtener id de subredes de la vpc

    @param vpc_id:
    @param session_client:
    @return: True si ..., si no False
    """
    try:
        client = session_client.client('ec2')
        response = client.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )
        resp = response['Subnets']
        tmp = []
        for i in resp:
            if "PrivateSubnet" in str(i):
                tmp.append(i.get("SubnetId"))
        if resp:
            print(tmp)
        else:
            print('No subnets found')
    except ClientError as e:
        logging.error(e)
        return False
    return tmp


def get_security_group_id(vpc_id, session_client):
    """Obtener id de grupos de seguridad de la vpc

    @param vpc_id:
    @param session_client:
    @return: True si ..., si no False
    """
    try:
        client = session_client.client('ec2')
        response = client.describe_security_groups(
            Filters=[
                dict(Name='vpc-id', Values=[vpc_id])
            ]
        )
        resp = response['SecurityGroups']
        tmp = []
        for i in resp:
            tmp.append(i.get("GroupId"))
        if resp:
            print(tmp)
        else:
            print('No subnets found')
    except ClientError as e:
        logging.error(e)
        return False
    else:
        return tmp
