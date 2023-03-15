import logging
from botocore.exceptions import ClientError

def create_datasource(session_client):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param session_client:
    :return: True if bucket created, else False
    """
    try:
        quick_sight = session_client.client('quicksight')
        response = quick_sight.create_data_source(
            AwsAccountId='string',
            DataSourceId='string',
            Name='string',
            Type='ADOBE_ANALYTICS' | 'AMAZON_ELASTICSEARCH' | 'ATHENA' | 'AURORA' | 'AURORA_POSTGRESQL' | 'AWS_IOT_ANALYTICS' | 'GITHUB' | 'JIRA' | 'MARIADB' | 'MYSQL' | 'ORACLE' | 'POSTGRESQL' | 'PRESTO' | 'REDSHIFT' | 'S3' | 'SALESFORCE' | 'SERVICENOW' | 'SNOWFLAKE' | 'SPARK' | 'SQLSERVER' | 'TERADATA' | 'TWITTER' | 'TIMESTREAM' | 'AMAZON_OPENSEARCH' | 'EXASOL' | 'DATABRICKS',
            DataSourceParameters={
                'AmazonElasticsearchParameters': {
                    'Domain': 'string'
                },
                'AthenaParameters': {
                    'WorkGroup': 'string',
                    'RoleArn': 'string'
                },
                'AuroraParameters': {
                    'Host': 'string',
                    'Port': 123,
                    'Database': 'string'
                },
                'AuroraPostgreSqlParameters': {
                    'Host': 'string',
                    'Port': 123,
                    'Database': 'string'
                },
                'AwsIotAnalyticsParameters': {
                    'DataSetName': 'string'
                },
                'JiraParameters': {
                    'SiteBaseUrl': 'string'
                },
                'MariaDbParameters': {
                    'Host': 'string',
                    'Port': 123,
                    'Database': 'string'
                },
                'MySqlParameters': {
                    'Host': 'string',
                    'Port': 123,
                    'Database': 'string'
                },
                'OracleParameters': {
                    'Host': 'string',
                    'Port': 123,
                    'Database': 'string'
                },
                'PostgreSqlParameters': {
                    'Host': 'string',
                    'Port': 123,
                    'Database': 'string'
                },
                'PrestoParameters': {
                    'Host': 'string',
                    'Port': 123,
                    'Catalog': 'string'
                },
                'RdsParameters': {
                    'InstanceId': 'string',
                    'Database': 'string'
                },
                'RedshiftParameters': {
                    'Host': 'string',
                    'Port': 123,
                    'Database': 'string',
                    'ClusterId': 'string'
                },
                'S3Parameters': {
                    'ManifestFileLocation': {
                        'Bucket': 'string',
                        'Key': 'string'
                    },
                    'RoleArn': 'string'
                },
                'ServiceNowParameters': {
                    'SiteBaseUrl': 'string'
                },
                'SnowflakeParameters': {
                    'Host': 'string',
                    'Database': 'string',
                    'Warehouse': 'string'
                },
                'SparkParameters': {
                    'Host': 'string',
                    'Port': 123
                },
                'SqlServerParameters': {
                    'Host': 'string',
                    'Port': 123,
                    'Database': 'string'
                },
                'TeradataParameters': {
                    'Host': 'string',
                    'Port': 123,
                    'Database': 'string'
                },
                'TwitterParameters': {
                    'Query': 'string',
                    'MaxRows': 123
                },
                'AmazonOpenSearchParameters': {
                    'Domain': 'string'
                },
                'ExasolParameters': {
                    'Host': 'string',
                    'Port': 123
                },
                'DatabricksParameters': {
                    'Host': 'string',
                    'Port': 123,
                    'SqlEndpointPath': 'string'
                }
            },
            Credentials={
                'CredentialPair': {
                    'Username': 'string',
                    'Password': 'string',
                    'AlternateDataSourceParameters': [
                        {
                            'AmazonElasticsearchParameters': {
                                'Domain': 'string'
                            },
                            'AthenaParameters': {
                                'WorkGroup': 'string',
                                'RoleArn': 'string'
                            },
                            'AuroraParameters': {
                                'Host': 'string',
                                'Port': 123,
                                'Database': 'string'
                            },
                            'AuroraPostgreSqlParameters': {
                                'Host': 'string',
                                'Port': 123,
                                'Database': 'string'
                            },
                            'AwsIotAnalyticsParameters': {
                                'DataSetName': 'string'
                            },
                            'JiraParameters': {
                                'SiteBaseUrl': 'string'
                            },
                            'MariaDbParameters': {
                                'Host': 'string',
                                'Port': 123,
                                'Database': 'string'
                            },
                            'MySqlParameters': {
                                'Host': 'string',
                                'Port': 123,
                                'Database': 'string'
                            },
                            'OracleParameters': {
                                'Host': 'string',
                                'Port': 123,
                                'Database': 'string'
                            },
                            'PostgreSqlParameters': {
                                'Host': 'string',
                                'Port': 123,
                                'Database': 'string'
                            },
                            'PrestoParameters': {
                                'Host': 'string',
                                'Port': 123,
                                'Catalog': 'string'
                            },
                            'RdsParameters': {
                                'InstanceId': 'string',
                                'Database': 'string'
                            },
                            'RedshiftParameters': {
                                'Host': 'string',
                                'Port': 123,
                                'Database': 'string',
                                'ClusterId': 'string'
                            },
                            'S3Parameters': {
                                'ManifestFileLocation': {
                                    'Bucket': 'string',
                                    'Key': 'string'
                                },
                                'RoleArn': 'string'
                            },
                            'ServiceNowParameters': {
                                'SiteBaseUrl': 'string'
                            },
                            'SnowflakeParameters': {
                                'Host': 'string',
                                'Database': 'string',
                                'Warehouse': 'string'
                            },
                            'SparkParameters': {
                                'Host': 'string',
                                'Port': 123
                            },
                            'SqlServerParameters': {
                                'Host': 'string',
                                'Port': 123,
                                'Database': 'string'
                            },
                            'TeradataParameters': {
                                'Host': 'string',
                                'Port': 123,
                                'Database': 'string'
                            },
                            'TwitterParameters': {
                                'Query': 'string',
                                'MaxRows': 123
                            },
                            'AmazonOpenSearchParameters': {
                                'Domain': 'string'
                            },
                            'ExasolParameters': {
                                'Host': 'string',
                                'Port': 123
                            },
                            'DatabricksParameters': {
                                'Host': 'string',
                                'Port': 123,
                                'SqlEndpointPath': 'string'
                            }
                        },
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
            VpcConnectionProperties={
                'VpcConnectionArn': 'string'
            },
            SslProperties={
                'DisableSsl': True | False
            },
            Tags=[
                {
                    'Key': 'string',
                    'Value': 'string'
                },
            ]
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True

def create_data_set( session_client=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param session_client:
    :return: True if bucket created, else False
    """
    try:
        quick = session_client.client("quicksight")
        response = quick.create_data_set(
            AwsAccountId='string',
            DataSetId='string',
            Name='string',
            PhysicalTableMap={
                'string': {
                    'RelationalTable': {
                        'DataSourceArn': 'string',
                        'Catalog': 'string',
                        'Schema': 'string',
                        'Name': 'string',
                        'InputColumns': [
                            {
                                'Name': 'string',
                                'Type': 'STRING' | 'INTEGER' | 'DECIMAL' | 'DATETIME' | 'BIT' | 'BOOLEAN' | 'JSON'
                            },
                        ]
                    },
                    'CustomSql': {
                        'DataSourceArn': 'string',
                        'Name': 'string',
                        'SqlQuery': 'string',
                        'Columns': [
                            {
                                'Name': 'string',
                                'Type': 'STRING' | 'INTEGER' | 'DECIMAL' | 'DATETIME' | 'BIT' | 'BOOLEAN' | 'JSON'
                            },
                        ]
                    },
                    'S3Source': {
                        'DataSourceArn': 'string',
                        'UploadSettings': {
                            'Format': 'CSV' | 'TSV' | 'CLF' | 'ELF' | 'XLSX' | 'JSON',
                            'StartFromRow': 123,
                            'ContainsHeader': True | False,
                            'TextQualifier': 'DOUBLE_QUOTE' | 'SINGLE_QUOTE',
                            'Delimiter': 'string'
                        },
                        'InputColumns': [
                            {
                                'Name': 'string',
                                'Type': 'STRING' | 'INTEGER' | 'DECIMAL' | 'DATETIME' | 'BIT' | 'BOOLEAN' | 'JSON'
                            },
                        ]
                    }
                }
            },
            LogicalTableMap={
                'string': {
                    'Alias': 'string',
                    'DataTransforms': [
                        {
                            'ProjectOperation': {
                                'ProjectedColumns': [
                                    'string',
                                ]
                            },
                            'FilterOperation': {
                                'ConditionExpression': 'string'
                            },
                            'CreateColumnsOperation': {
                                'Columns': [
                                    {
                                        'ColumnName': 'string',
                                        'ColumnId': 'string',
                                        'Expression': 'string'
                                    },
                                ]
                            },
                            'RenameColumnOperation': {
                                'ColumnName': 'string',
                                'NewColumnName': 'string'
                            },
                            'CastColumnTypeOperation': {
                                'ColumnName': 'string',
                                'NewColumnType': 'STRING' | 'INTEGER' | 'DECIMAL' | 'DATETIME',
                                'Format': 'string'
                            },
                            'TagColumnOperation': {
                                'ColumnName': 'string',
                                'Tags': [
                                    {
                                        'ColumnGeographicRole': 'COUNTRY' | 'STATE' | 'COUNTY' | 'CITY' | 'POSTCODE' | 'LONGITUDE' | 'LATITUDE',
                                        'ColumnDescription': {
                                            'Text': 'string'
                                        }
                                    },
                                ]
                            },
                            'UntagColumnOperation': {
                                'ColumnName': 'string',
                                'TagNames': [
                                    'COLUMN_GEOGRAPHIC_ROLE' | 'COLUMN_DESCRIPTION',
                                ]
                            }
                        },
                    ],
                    'Source': {
                        'JoinInstruction': {
                            'LeftOperand': 'string',
                            'RightOperand': 'string',
                            'LeftJoinKeyProperties': {
                                'UniqueKey': True | False
                            },
                            'RightJoinKeyProperties': {
                                'UniqueKey': True | False
                            },
                            'Type': 'INNER' | 'OUTER' | 'LEFT' | 'RIGHT',
                            'OnClause': 'string'
                        },
                        'PhysicalTableId': 'string',
                        'DataSetArn': 'string'
                    }
                }
            },
            ImportMode='SPICE' | 'DIRECT_QUERY',
            ColumnGroups=[
                {
                    'GeoSpatialColumnGroup': {
                        'Name': 'string',
                        'CountryCode': 'US',
                        'Columns': [
                            'string',
                        ]
                    }
                },
            ],
            FieldFolders={
                'string': {
                    'description': 'string',
                    'columns': [
                        'string',
                    ]
                }
            },
            Permissions=[
                {
                    'Principal': 'string',
                    'Actions': [
                        'string',
                    ]
                },
            ],
            RowLevelPermissionDataSet={
                'Namespace': 'string',
                'Arn': 'string',
                'PermissionPolicy': 'GRANT_ACCESS' | 'DENY_ACCESS',
                'FormatVersion': 'VERSION_1' | 'VERSION_2',
                'Status': 'ENABLED' | 'DISABLED'
            },
            RowLevelPermissionTagConfiguration={
                'Status': 'ENABLED' | 'DISABLED',
                'TagRules': [
                    {
                        'TagKey': 'string',
                        'ColumnName': 'string',
                        'TagMultiValueDelimiter': 'string',
                        'MatchAllValue': 'string'
                    },
                ]
            },
            ColumnLevelPermissionRules=[
                {
                    'Principals': [
                        'string',
                    ],
                    'ColumnNames': [
                        'string',
                    ]
                },
            ],
            Tags=[
                {
                    'Key': 'string',
                    'Value': 'string'
                },
            ],
            DataSetUsageConfiguration={
                'DisableUseAsDirectQuerySource': True | False,
                'DisableUseAsImportedSource': True | False
            }
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True
