import logging
import time

from botocore.exceptions import ClientError


def create_bucket(bucket_name, region=None, session_client=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param session_client:
    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    try:
        s3_client = session_client.client('s3')
        if region is None:
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        print("Creating...")
        time.sleep(5)
        print("Create susses bucket", bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(file_name, bucket, object_name, session_client):
    """Upload a file to an S3 bucket

    :param session_client:
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    try:
        s3_client = session_client.client('s3')
        response = s3_client.upload_file(file_name, bucket, object_name)
        print("Upload susses dags files", f"{file_name}/{object_name}/{file_name}")
    except ClientError as e:
        logging.error(e)
        return False
    return True


def put_public_access_block(bucket, session_client):
    """Put block public access to bucket S3

    :param session_client:
    :param bucket: Bucket to upload to
    :return: True if file was uploaded, else False
    """
    try:
        s3_client = session_client.client('s3')
        s3_client.put_public_access_block(
            Bucket=bucket,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': True,
                'IgnorePublicAcls': True,
                'BlockPublicPolicy': True,
                'RestrictPublicBuckets': True
            }
        )

    except ClientError as e:
        logging.error(e)
        return False
    return True


def deleted_buckets(session_client, bucket_name):
    """Put block public access to bucket S3

    :param bucket_name:
    :param session_client:
    :return: True if file was uploaded, else False
    """
    try:
        s3_client = session_client.client('s3')
        s3_client.delete_bucket(
            Bucket=bucket_name
        )
        print("Deleted susses bucket", bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
