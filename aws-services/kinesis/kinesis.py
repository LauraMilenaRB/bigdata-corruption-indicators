"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import logging
import time
from botocore.exceptions import ClientError
import iam


def create_stream_kinesis(session_client, kinesis_stream_name):
    """Creación elasticmapreduce_streaming kinesis

    @param session_client:
    @param kinesis_stream_name:
    @return: True si ..., si no False
    """
    try:
        client = session_client.client("kinesis")
        response = client.create_stream(
            StreamName=kinesis_stream_name,
            StreamModeDetails={
                'StreamMode': 'ON_DEMAND'
            }
        )
    except ClientError as e:
        logging.error(e)
        return False
    else:
        print(f"Creando elasticmapreduce_streaming kinesis {kinesis_stream_name}...")
        time.sleep(10)
        print(f"Creado elasticmapreduce_streaming kinesis {kinesis_stream_name} con éxito")
        return True


def delete_stream_kinesis(session_client, kinesis_stream_name):
    """Create an S3 bucket in a specified region

    @param kinesis_stream_name:
    @param session_client:
    @return: True si ..., si no False
    """
    try:
        client = session_client.client("kinesis")
        response = client.delete_stream(
            StreamName=kinesis_stream_name,
            EnforceConsumerDeletion=True
            # StreamARN='string'
        )
    except ClientError as e:
        logging.error(e)
        return False
    else:
        print(f"Eliminando elasticmapreduce_streaming kinesis {kinesis_stream_name}...")
        time.sleep(5)
        print(f"Eliminado elasticmapreduce_streaming kinesis {kinesis_stream_name} con éxito")
        return True


def delete_delivery_stream_kinesis(session_client, kinesis_delivery_stream_name):
    """Eliminación delivery elasticmapreduce_streaming kinesis

    @param kinesis_delivery_stream_name:
    @param session_client:
    @return: True si ..., si no False
    """
    try:
        client = session_client.client("firehose")
        response = client.delete_delivery_stream(
            DeliveryStreamName=kinesis_delivery_stream_name,
            AllowForceDelete=True
        )
    except ClientError as e:
        logging.error(e)
        return False
    else:
        print(f"Eliminando delivery elasticmapreduce_streaming kinesis {kinesis_delivery_stream_name}...")
        time.sleep(60)
        print(f"Eliminado delivery elasticmapreduce_streaming kinesis {kinesis_delivery_stream_name} con éxito")
        return True


def create_role_kinesis(session_client, kinesis_delivery_stream_name):
    """Creación rol para kinesis

    @param kinesis_delivery_stream_name:
    @param session_client:
    @return: True si ..., si no False
    """
    try:
        identity = session_client.client('sts').get_caller_identity()

        file = open("aws-services/kinesis/stream_kinesis_policy_doc.json", "r")
        ct_file = file.read() \
            .replace("{your-account-id}", identity.get("Account")) \
            .replace("{your-region}", session_client.region_name)
        file.close()
        policy_name = f'{kinesis_delivery_stream_name}-policy'
        role_name = f'{kinesis_delivery_stream_name}-role'

        iam.create_policy(session_client, policy_name, ct_file)
        iam.create_role(session_client, role_name,
                        open("aws-services/kinesis/assume_stream_kinesis_policy_doc.json").read())
        iam.attach_role_policy(session_client, role_name,
                               f'arn:aws:iam::{identity.get("Account")}:policy/{policy_name}')

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print(f"Creando rol delivery elasticmapreduce_streaming kinesis {kinesis_delivery_stream_name}...")
        time.sleep(10)
        print(f"Creado role delivery elasticmapreduce_streaming kinesis {kinesis_delivery_stream_name} con éxito")
        return True


def deleted_role_kinesis(session_client, kinesis_delivery_stream_name):
    """Eliminación rol para kinesis

    @param kinesis_delivery_stream_name:
    @param session_client:
    @return: True si ..., si no False
    """
    try:
        policy_name = f'{kinesis_delivery_stream_name}-policy'
        role_name = f'{kinesis_delivery_stream_name}-role'

        iam.detach_role_policy(session_client, policy_name, role_name)
        time.sleep(5)
        iam.delete_role(session_client, role_name)
        time.sleep(5)

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Eliminando rol para delivery elasticmapreduce_streaming kinesis...")
        time.sleep(5)
        print("Eliminado role delivery elasticmapreduce_streaming kinesis con éxito")
        return True


def create_delivery_stream_kinesis(session_client, kinesis_delivery_stream_name, kinesis_stream_name,
                                   s3_output_destination, key_staging_bucket, column_partition_key):
    """Creación del servicio delivery elasticmapreduce_streaming kinesis

    @param session_client:
    @param kinesis_delivery_stream_name:
    @param kinesis_stream_name:
    @param s3_output_destination:
    @param key_staging_bucket:
    @param column_partition_key:
    @return: True si ..., si no False
    """
    try:
        identity = session_client.client('sts').get_caller_identity()
        client = session_client.client("firehose")
        role_name = f'{kinesis_delivery_stream_name}-role'
        account_id = identity.get('Account')
        response = client.create_delivery_stream(
            DeliveryStreamName=kinesis_delivery_stream_name,
            DeliveryStreamType='KinesisStreamAsSource',
            KinesisStreamSourceConfiguration={
                'KinesisStreamARN': f"arn:aws:kinesis:{session_client.region_name}:{account_id}:elasticmapreduce_streaming/{kinesis_stream_name}",
                'RoleARN': f'arn:aws:iam::{account_id}:role/{role_name}'
            },
            ExtendedS3DestinationConfiguration={
                'RoleARN': f'arn:aws:iam::{account_id}:role/{role_name}',
                'BucketARN': f'arn:aws:s3:::{s3_output_destination}',
                'Prefix': key_staging_bucket + "/!{partitionKeyFromQuery:event_date}/",
                'ErrorOutputPrefix': 'log-error-',
                'BufferingHints': {
                    'SizeInMBs': 128,
                    'IntervalInSeconds': 100
                },
                'CompressionFormat': 'HADOOP_SNAPPY',
                'CloudWatchLoggingOptions': {
                    'Enabled': False
                },
                'DynamicPartitioningConfiguration': {
                    'Enabled': True,
                    'RetryOptions': {
                        'DurationInSeconds': 300
                    }
                },
                'ProcessingConfiguration': {
                    'Enabled': True,
                    'Processors': [
                        {
                            'Type': 'MetadataExtraction',
                            'Parameters': [
                                {
                                    'ParameterName': 'JsonParsingEngine',
                                    'ParameterValue': 'JQ-1.6'
                                },
                                {
                                    'ParameterName': 'MetadataExtractionQuery',
                                    'ParameterValue': column_partition_key
                                }
                            ]
                        }
                    ]
                }
            }
        )
    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Creando delivery elasticmapreduce_streaming kinesis...")
        time.sleep(60)
        print("Creado delivery elasticmapreduce_streaming kinesis con éxito")
        return True