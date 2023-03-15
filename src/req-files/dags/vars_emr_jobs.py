from datetime import date, datetime

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2023, 3, 8),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "youremail@host.com",
    "retries": 1
}


endpoint_local_arg = {}
bucket_staging_name_arg = "test-pgr-staging-zone"
bucket_raw_name_arg = "test-pgr-raw-zone"
bucket_master_name_arg = "test-pgr-curated-zone"
bucket_scripts_name_arg = "test-pgr-req-files"
data_type_origin_arg = "json"
date_load_data = str(date.today())
id_date_load_data = str(date.today()).replace("-", "")

endpoint_url_arg = {
    "t_seii_procecotrata_compraadjudi": "https://www.datos.gov.co/resource/p6dx-8zbt.json",
    "t_seii_paecontratac_procesocontr": "https://www.datos.gov.co/resource/xhza-un65.json",
    "t_tvec_tiendavircol_estcolconsol": "https://www.datos.gov.co/resource/rgxm-mmea.json",
    "t_seii_contracanela_aislamiencon": "https://www.datos.gov.co/resource/8uyg-q2s6.json",
    "t_otro_pernajuesadl_camarcomerci": "https://www.datos.gov.co/resource/c82u-588k.json",
    "t_seii_ofertaproces_procesocompr": "https://www.datos.gov.co/resource/wi7w-2nvm.json"
}

ind_sources = {
    "ind_abuso_contratacion": f"t_seii_procecotrata_compraadjudi=s3://{bucket_raw_name_arg}/t_seii_procecotrata_compraadjudi/",
    "ind_ofertas_costosas": f"t_seii_procecotrata_compraadjudi=s3://{bucket_raw_name_arg}/t_seii_procecotrata_compraadjudi/,"
                            f"t_seii_ofertaproces_procesocompr=s3://{bucket_raw_name_arg}/t_seii_ofertaproces_procesocompr/"
}

JOB_FLOW_OVERRIDES ={
    'Name': 'mwaa-emr-cluster',
    'ReleaseLabel': 'emr-5.36.0',
    'LogUri': 's3://test-pgr-aws-logs/',
    'Applications': [
        {
            'Name': 'Spark'
        }
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'MASTER',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceCount': 1,
                'InstanceType': 'm5.xlarge',
            },
            {
                'Name': 'Core node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            }
        ],
        'Ec2SubnetId': 'subnet-02fe631d69b24cc75',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    "StepConcurrencyLevel": 5,
    'Tags': [
        {
            'Key': 'Name',
            'Value': 'MWAA Blogpost Cluster'
        }
    ]

}
athena_database = "default"
output_location_athena = "aws-logs-query"
DDL_results = " CREATE EXTERNAL TABLE t_result_indicadores_batch (" \
             "cantidad_irregularidades bigint," \
             "cantidad_contratos_irregularidades bigint," \
             "monto_total_irregularidades decimal(30,3)," \
             "cantidad_contratos_totales bigint)" \
             "PARTITIONED BY (fecha_ejecucion date,nombre_indicador string,nombre_grupo_indicador string)" \
             "LOCATION 's3://test-pgr-curated-zone/t_result_indicadores_batch/';"



