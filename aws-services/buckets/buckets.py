"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import logging
import time
from botocore.exceptions import ClientError


def create_bucket(bucket_name, region=None, session_client=None):
    """Creación del bucket S3

    @param session_client: Sesión AWS
    @param bucket_name: Nombre del bucket a crear
    @param region: Region donde se creara el bucket
    @return: True si el bucket se crea, si no False
    """

    try:
        s3_client = session_client.client('s3')
        if region is None:
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print(f"Creando bucket {bucket_name}...")
        time.sleep(5)
        print(f"Creado bucket {bucket_name} con éxito")
        return True


def upload_file(file_name, bucket, object_name, session_client):
    """Subir un archivo a un depósito S3

    @param session_client: Sesión AWS
    @param bucket: Nombre del bucket actualizar
    @param file_name: Archivo a subir
    @param object_name: Nombre del objeto S3. Si no se especifica, se utiliza file_name
    @return: True si el archivo en el bucket se actualiza, si no False
    """
    try:
        s3_client = session_client.client('s3')
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    else:
        print(f"Archivos dags {file_name}/{object_name}/{file_name} subido con éxito")
        return True


def put_public_access_block(bucket, session_client):
    """Poner bloque de acceso público al depósito S3

    @param session_client: Sesión AWS
    @param bucket: Nombre del bucket a crear
    @return: True si al bucket se le da acceso, si no False
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
    """Eliminación de buckets

    @param session_client: Sesión AWS
    @param bucket_name: Nombre del bucket a crear
    @return: True si el bucket se elimina, si no False
    """
    try:
        s3_client = session_client.client('s3')
        s3_client.delete_bucket(
            Bucket=bucket_name
        )

    except ClientError as e:
        logging.error(e)
        return False
    else:
        print("Eliminación del bucket {bucket_name} con éxito")
    return True
