import logging
import time

from botocore.exceptions import ClientError


def deleted_stack_template_vpc(session_client, stack_vpc_name):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param stack_vpc_name:
    :param session_client:
    :return: True if bucket created, else False
    """
    try:
        cloudformation_client = session_client.client('cloudformation')
        cloudformation_client.delete_stack(
            StackName=stack_vpc_name
        )
        print("Delete...")
        time.sleep(15)
        print("Deleted stack vpc")
    except ClientError as e:
        logging.error(e)
        return False
    return True


def created_default_vpc(session_client):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param session_client:
    :return: True if bucket created, else False
    """
    try:
        my_region = session_client.region_name
        session_client.client("ec2", region_name=my_region).create_default_vpc()
    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Creating VPC default...")
        time.sleep(30)
        print("Create VPC default success")
        return True


def create_stack_template_vpc(stack_vpc_name, path_file, capabilities_par, session_client, conf_var):
    """Create an Stack Cloudformation

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param conf_var:
    :param stack_vpc_name:
    :param session_client:
    :param capabilities_par:
    :param path_file:
    :return: True if bucket created, else False
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
        print("Creating VPC...")
        time.sleep(90)
        print("Create stack vpc success")
        return True


def deleted_vpc(session_client, vpc_id):
    """Delete the vpc

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param vpc_id:
    :param session_client:
    :return: True if bucket created, else False
    """
    ec2 = session_client.resource('ec2')
    vpc_resource = ec2.Vpc(vpc_id)
    try:
        print("Removing vpc-id: ", vpc_resource.id)
        vpc_resource.delete()
    except ClientError as e:
        logging.error(e)
        print("Please remove dependencies and delete VPC manually.")


def deleted_acl(session_client, vpc_id):
    """Delete the network-access-lists

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param vpc_id:
    :param session_client:
    :return: True if bucket created, else False
    """
    ec2 = session_client.resource('ec2')
    vpc_resource = ec2.Vpc(vpc_id)
    acls = vpc_resource.network_acls.all()
    if acls:
        try:
            for acl in acls:
                if acl.is_default:
                    print(acl.id + " is the default NACL, continue...")
                    continue
                print("Removing acl-id: ", acl.id)
                acl.delete()
        except ClientError as e:
            logging.error(e)


def deleted_seg_group(session_client, vpc_id):
    """Delete any security-groups

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param vpc_id:
    :param session_client:
    :return: True if bucket created, else False
    """

    ec2 = session_client.resource('ec2')
    vpc_resource = ec2.Vpc(vpc_id)
    sgps = vpc_resource.security_groups.all()
    if sgps:
        try:
            for sg in sgps:
                if sg.group_name == 'default':
                    print(sg.id + " is the default security group, continue...")
                    continue
                print("Removing sg-id: ", sg.id)
                sg.delete()
        except ClientError as e:
            logging.error(e)


def deleted_subnets(session_client, vpc_id):
    """Delete the subnets

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param vpc_id:
    :param session_client:
    :return: True if bucket created, else False
    """
    ec2 = session_client.resource('ec2')
    vpc_resource = ec2.Vpc(vpc_id)
    subnets = vpc_resource.subnets.all()
    default_subnets = [ec2.Subnet(subnet.id) for subnet in subnets if subnet.default_for_az]
    if default_subnets:
        try:
            for sub in default_subnets:
                print("Removing sub-id: ", sub.id)
                sub.delete()
        except ClientError as e:
            logging.error(e)


def deleted_int_gw(session_client, vpc_id):
    """Detach and delete the internet-gateway

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param vpc_id:
    :param session_client:
    :return: True if bucket created, else False
    """
    ec2 = session_client.resource('ec2')
    vpc_resource = ec2.Vpc(vpc_id)
    igws = vpc_resource.internet_gateways.all()
    if igws:
        for igw in igws:
            try:
                print("Detaching and Removing igw-id: ", igw.id)
                igw.detach_from_vpc(
                    VpcId=vpc_id
                )
                igw.delete()
            except ClientError as e:
                logging.error(e)


def deleted_route_tables(session_client, vpc_id):
    """Delete the route-tables

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param vpc_id:
    :param session_client:
    :return: True if bucket created, else False
    """
    ec2 = session_client.resource('ec2')
    vpc_resource = ec2.Vpc(vpc_id)
    rtbs = vpc_resource.route_tables.all()
    if rtbs:
        try:
            for rtb in rtbs:
                assoc_attr = [rtb.associations_attribute for rtb in rtbs]
                if [rtb_ass[0]['RouteTableId'] for rtb_ass in assoc_attr if rtb_ass[0]['Main'] == True]:
                    print(rtb.id + " is the main route table, continue...")
                    continue
                print("Removing rtb-id: ", rtb.id)
                table = ec2.RouteTable(rtb.id)
                table.delete()
        except ClientError as e:
            logging.error(e)


def deleted_vpc_all_dep(vpc_id, session_client):
    """    Do the work - order of operation
    1.) Delete the internet-gateway
    2.) Delete subnets
    3.) Delete route-tables
    4.) Delete network access-lists
    5.) Delete security-groups
    6.) Delete the VPC

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param vpc_id:
    :param session_client:
    :return: True if bucket created, else False
    """
    deleted_int_gw(session_client, vpc_id)
    deleted_subnets(session_client, vpc_id)
    deleted_route_tables(session_client, vpc_id)
    deleted_acl(session_client, vpc_id)
    deleted_seg_group(session_client, vpc_id)
    deleted_vpc(session_client, vpc_id)


def get_vpc_id(stack_vpc_name, session_client, vpcCIDR):
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
    return resp


def get_private_subnets_id(vpc_id, session_client):
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
    return tmp
