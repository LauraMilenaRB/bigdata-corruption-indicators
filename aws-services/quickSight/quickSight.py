"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import logging
from botocore.exceptions import ClientError


def create_datasource(session_client):
    """Creación data source QuickSight

    @param session_client:
    @return: True si ..., si no False
    """
    try:
        quick_sight = session_client.client('quicksight')
        response = quick_sight.create_data_source(
            AwsAccountId='string',
            DataSourceId='string',
            Name='string',
            Type='REDSHIFT',
            DataSourceParameters={
                'RedshiftParameters': {
                    'Host': 'string',
                    'Port': 123,
                    'Database': 'string',
                    'ClusterId': 'string'
                }
            },
            Credentials={
                'CredentialPair': {
                    'Username': 'string',
                    'Password': 'string',
                    'AlternateDataSourceParameters': [
                        {
                            'RedshiftParameters': {
                                'Host': 'string',
                                'Port': 123,
                                'Database': 'string',
                                'ClusterId': 'string'
                            }
                        }
                    ]
                },
                'CopySourceArn': 'string',
                'SecretArn': 'string'
            },
            Permissions=[
                {
                    'Principal': 'string',
                    'Actions': [
                        'string',
                    ]
                },
            ],
            SslProperties={
                'DisableSsl': False
            }
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True
