"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

from datetime import date, datetime

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2023, 3, 12),
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
    "t_otro_pernajuesadl_camarcomerci": "https://www.datos.gov.co/resource/c82u-588k.json",
    "t_seii_ofertaproces_procesocompr": "https://www.datos.gov.co/resource/wi7w-2nvm.json"
}
"""
endpoint_url_arg2 = {
    "t_seii_procecotrata_compraadjudi": "https://www.datos.gov.co/resource/p6dx-8zbt.json",
    "t_seii_contracanela_aislamiencon": "https://www.datos.gov.co/resource/8uyg-q2s6.json"
    "t_otro_pernajuesadl_camarcomerci": "https://www.datos.gov.co/resource/c82u-588k.json",
    "t_seii_ofertaproces_procesocompr": "https://www.datos.gov.co/resource/wi7w-2nvm.json"
}
"""
objects = [
    "t_seii_procecotrata_compraadjudi",
    "t_seii_contracanela_aislamiencon",
    "t_otro_pernajuesadl_camarcomerci",
    "t_otro_persexpupoli_sigepperexpo",
    "t_seii_ofertaproces_procesocompr",
    "t_otro_puestsensibl_sigeppsscorr",
    "t_seii_multasysanci_secopiimulsa",
    "t_paco_registro_obras_inconclusa",
    "t_paco_responsabilidad_fiscales",
    "t_seii_ejecucioncon_avancerevses"
]

ind_sources = {
    "ind_abuso_contratacion": f"t_seii_procecotrata_compraadjudi=s3://{bucket_raw_name_arg}/t_seii_procecotrata_compraadjudi/",
    "ind_ofertas_costosas": f"t_seii_procecotrata_compraadjudi=s3://{bucket_raw_name_arg}/t_seii_procecotrata_compraadjudi/,"
                            f"t_seii_ofertaproces_procesocompr=s3://{bucket_raw_name_arg}/t_seii_ofertaproces_procesocompr/",
    "ind_contratos_prov_inactivos": f"t_otro_pernajuesadl_camarcomerci=s3://{bucket_raw_name_arg}/t_otro_pernajuesadl_camarcomerci/,"
                                    f"t_seii_procecotrata_compraadjudi=s3://{bucket_raw_name_arg}/t_seii_procecotrata_compraadjudi/",
    "ind_contratos_prov_PEP": f"t_otro_persexpupoli_sigepperexpo=s3://{bucket_raw_name_arg}/t_otro_persexpupoli_sigepperexpo/,"
                              f"t_seii_procecotrata_compraadjudi=s3://{bucket_raw_name_arg}/t_seii_procecotrata_compraadjudi/",
    "ind_contratos_prov_pust_sensibles": f"t_otro_puestsensibl_sigeppsscorr=s3://{bucket_raw_name_arg}/t_otro_puestsensibl_sigeppsscorr/,"
                                         f"t_seii_procecotrata_compraadjudi=s3://{bucket_raw_name_arg}/t_seii_procecotrata_compraadjudi/",
    "ind_contratistas_contratos_cancel": f"t_seii_contracanela_aislamiencon=s3://{bucket_raw_name_arg}/t_seii_contracanela_aislamiencon/,"
                                         f"t_seii_procecotrata_compraadjudi=s3://{bucket_raw_name_arg}/t_seii_procecotrata_compraadjudi/",
    "ind_contratos_incumplimiento_entregas": f"t_seii_ejecucioncon_avancerevses=s3://{bucket_raw_name_arg}/t_seii_ejecucioncon_avancerevses/,"
                                             f"t_seii_procecotrata_compraadjudi=s3://{bucket_raw_name_arg}/t_seii_procecotrata_compraadjudi/",
    "ind_inhabilitados_multas": f"t_seii_multasysanci_secopiimulsa=s3://{bucket_raw_name_arg}/t_seii_multasysanci_secopiimulsa/,"
                                f"t_seii_procecotrata_compraadjudi=s3://{bucket_raw_name_arg}/t_seii_procecotrata_compraadjudi/",
    "ind_inhabilitados_obras_inconclusas": f"t_paco_registro_obras_inconclusa=s3://{bucket_raw_name_arg}/t_paco_registro_obras_inconclusa/,"
                                           f"t_seii_procecotrata_compraadjudi=s3://{bucket_raw_name_arg}/t_seii_procecotrata_compraadjudi/",
    "ind_inhabilitados_resp_fiscal": f"t_paco_responsabilidad_fiscales=s3://{bucket_raw_name_arg}/t_paco_responsabilidad_fiscales/,"
                                           f"t_seii_procecotrata_compraadjudi=s3://{bucket_raw_name_arg}/t_seii_procecotrata_compraadjudi/",
}

JOB_FLOW_OVERRIDES = {
    'Name': 'mwaa-emr-cluster',
    'ReleaseLabel': 'emr-5.36.0',
    'LogUri': 's3://test-pgr-aws-logs/elasticmapreduce/',
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
        'Ec2SubnetId': 'subnet-0c5489de6687572e6',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
    'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
    'StepConcurrencyLevel': 8,
    'Tags': [
        {
            'Key': 'Name',
            'Value': 'MWAA Blogpost Cluster'
        }
    ]

}

endpoint_conn_arg = "amazonredshift-indicadores-cluster-1.c3ss5hvfzcj7.us-east-1.redshift.amazonaws.com"
bd_name_arg = "bd_contracts"
user_db_arg = "user-redshift-admin"
psw_db_arg = "Redshift123"
deleted_data_results = "delete from t_result_indicadores_batch;"
insert_data_results = f"copy t_result_indicadores_batch from 's3://test-pgr-curated-zone/t_result_indicadores_batch/fecha_ejecucion={date_load_data}' " \
                      f"iam_role 'arn:aws:iam::354824231875:role/AmazonRedshift-indicadores-role' format as json 'auto';"



