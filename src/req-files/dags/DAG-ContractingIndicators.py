"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import logging
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator

import requests
from vars_emr_jobs import *


def download_dataset(**context):
    endpoint_url = context['endpoint_url_arg']
    endpoint_local = context['endpoint_local_arg']
    bucket_name = context['bucket_name_arg']
    data_type_origin = context['data_type_origin_arg']

    s3_hook = S3Hook(aws_conn_id='aws_default')

    if len(endpoint_url) > 0:
        for key_source, origin_path in endpoint_url.items():
            origin_data = requests.get(origin_path)
            if origin_data:
                bucket = s3_hook.get_bucket(bucket_name)
                bucket.put_object(Bucket=bucket_name,
                                  Key=f'{key_source}/{key_source}_{id_date_load_data}.{data_type_origin}',
                                  Body=origin_data.content)
            if s3_hook.check_for_key(bucket_name=bucket_name, key=f'{key_source}/{id_date_load_data}_con éxito'):
                s3_hook.delete_objects(bucket=bucket_name, keys=f'{key_source}/{id_date_load_data}_con éxito')
            s3_hook.load_string(bucket_name=bucket_name, key=f'{key_source}/{id_date_load_data}_con éxito',
                                string_data="con éxito")

        return True
    else:
        return False


def steps_etl():
    dict_steps = {}
    if len(objects) > 0:
        for key_step in objects:
            dict_steps[key_step] = {
                'Name': f'ingestion_process_{key_step}',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit',
                             '--master', 'yarn',
                             '--deploy-mode', 'client', f's3://{bucket_scripts_name_arg}/scripts/etl_{key_step}.py',
                             '--staging_bucket', bucket_staging_name_arg,
                             '--raw_bucket', bucket_raw_name_arg,
                             '--key', key_step,
                             '--date_origin', id_date_load_data,
                             '--app_name', f'app-{key_step}'
                             ]
                }
            }
    return dict_steps


def steps_ind():
    dict_steps = {}
    if len(endpoint_url_arg) > 0:
        for key_step, v in ind_sources.items():
            dict_steps[key_step] = {
                'Name': f'process_{key_step}',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit',
                             '--master', 'yarn',
                             '--deploy-mode', 'client', f's3://{bucket_scripts_name_arg}/scripts/{key_step}.py',
                             '--sources', v,
                             '--destination_bucket', bucket_master_name_arg,
                             '--key', key_step,
                             '--date_origin', date_load_data,
                             '--app_name', f'app-{key_step}'
                             ]
                }
            }
    return dict_steps


def create_query_redshift(**context):
    """Create a role execution environment for MWAA

    

    @return: True si ..., si no False
    """
    try:
        query = context['query_arg']
        password_db = context['password_db_arg']
        end_point = context['end_point_arg']
        name_bd = context['name_bd_arg']
        username_db = context['username_db_arg']
        print(query, end_point, username_db, password_db, name_bd)
        conn = psycopg2.connect(
            host=end_point,
            port=5439,
            user=username_db,
            password=password_db,
            database=name_bd
        )

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        conn.close()

    except Exception as e:
        logging.error(e)
        return False
    else:
        return True


with DAG(dag_id='mwaa_pipeline_contratacion_publica', schedule_interval="0 0 * * 0",
         default_args=default_args, catchup=False, tags=['emr', 'mwaa']) as dag:

    download_origin_data = PythonOperator(
        task_id='download_origin',
        python_callable=download_dataset,
        provide_context=True,
        op_kwargs={'endpoint_url_arg': endpoint_url_arg, 'endpoint_local_arg': endpoint_local_arg,
                   'bucket_name_arg': bucket_staging_name_arg, 'data_type_origin_arg': data_type_origin_arg},
    )

    check_download_bucket = []

    for key, value in endpoint_url_arg.items():
        check_staging_s3_bucket = S3KeySensor(
            task_id=f'check_staging_{key}',
            aws_conn_id='aws_default',
            bucket_name=bucket_staging_name_arg,
            bucket_key=f'{key}/{id_date_load_data}_con éxito'
        )
        check_download_bucket.append(check_staging_s3_bucket)

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        aws_conn_id='aws_default',
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    emr_step_jobs_etl = {}
    emr_step_sensor_etl = {}

    steps_etls = steps_etl()

    for key, step in steps_etls.items():
        add_emr_spark_step = EmrAddStepsOperator(
            task_id=f'emr_step_etl_{key}',
            aws_conn_id='aws_default',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
            steps=[step]
        )
        emr_step_jobs_etl[key] = add_emr_spark_step

        emr_spark_job_sensor = EmrStepSensor(
            task_id=f'emr_job_sensor_etl_{key}',
            aws_conn_id='aws_default',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
            step_id=f"{{{{ task_instance.xcom_pull(task_ids='emr_step_etl_{key}')[0] }}}}"
        )
        emr_step_sensor_etl[key] = emr_spark_job_sensor

    steps_inds = steps_ind()

    emr_step_jobs_indic = {}
    emr_step_sensor_indic = {}

    for key, step in steps_inds.items():
        add_emr_spark_step = EmrAddStepsOperator(
            task_id=f'emr_step_process_{key}',
            aws_conn_id='aws_default',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
            steps=[step]
        )
        emr_step_jobs_indic[key] = add_emr_spark_step

        emr_spark_job_sensor = EmrStepSensor(
            task_id=f'emr_job_sensor_process_{key}',
            aws_conn_id='aws_default',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
            step_id=f"{{{{ task_instance.xcom_pull(task_ids='emr_step_process_{key}')[0] }}}}"
        )
        emr_step_sensor_indic[key] = emr_spark_job_sensor

    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}"
    )

    task_deleted_data = PythonOperator(
        task_id='task_deleted_data',
        python_callable=create_query_redshift,
        provide_context=True,
        op_kwargs={'query_arg': deleted_data_results, 'password_db_arg': psw_db_arg, 'end_point_arg': endpoint_conn_arg,
                   'name_bd_arg': bd_name_arg, 'username_db_arg': user_db_arg},
    )

    task_insert_data = PythonOperator(
        task_id='task_insert_data',
        python_callable=create_query_redshift,
        provide_context=True,
        op_kwargs={'query_arg': insert_data_results, 'password_db_arg': psw_db_arg, 'end_point_arg': endpoint_conn_arg,
                   'name_bd_arg': bd_name_arg, 'username_db_arg': user_db_arg},
    )


    download_origin_data >> [sensor_staging_bucket for sensor_staging_bucket in check_download_bucket] >> create_emr_cluster

    [create_emr_cluster >> step >> sensor for step, sensor in zip(emr_step_jobs_etl.values(), emr_step_sensor_etl.values())]

    emr_step_sensor_etl["t_seii_procecotrata_compraadjudi"] >> emr_step_jobs_indic["ind_abuso_contratacion"] >> \
    emr_step_sensor_indic["ind_abuso_contratacion"] >> [remove_cluster, task_deleted_data]

    [emr_step_sensor_etl["t_seii_procecotrata_compraadjudi"],
     emr_step_sensor_etl["t_seii_ofertaproces_procesocompr"]] >> \
    emr_step_jobs_indic["ind_ofertas_costosas"] >> emr_step_sensor_indic["ind_ofertas_costosas"] >> [remove_cluster, task_deleted_data]

    [emr_step_sensor_etl["t_otro_pernajuesadl_camarcomerci"],
     emr_step_sensor_etl["t_seii_procecotrata_compraadjudi"]] >> \
    emr_step_jobs_indic["ind_contratos_prov_inactivos"] >> emr_step_sensor_indic["ind_contratos_prov_inactivos"] >> [remove_cluster, task_deleted_data]

    [emr_step_sensor_etl["t_otro_persexpupoli_sigepperexpo"],
     emr_step_sensor_etl["t_seii_procecotrata_compraadjudi"]] >> \
    emr_step_jobs_indic["ind_contratos_prov_PEP"] >> emr_step_sensor_indic["ind_contratos_prov_PEP"] >> [remove_cluster, task_deleted_data]

    [emr_step_sensor_etl["t_otro_puestsensibl_sigeppsscorr"],
     emr_step_sensor_etl["t_seii_procecotrata_compraadjudi"]] >> \
    emr_step_jobs_indic["ind_contratos_prov_pust_sensibles"] >> emr_step_sensor_indic["ind_contratos_prov_pust_sensibles"] >> [remove_cluster, task_deleted_data]

    [emr_step_sensor_etl["t_seii_contracanela_aislamiencon"],
     emr_step_sensor_etl["t_seii_procecotrata_compraadjudi"]] >> \
    emr_step_jobs_indic["ind_contratistas_contratos_cancel"] >> emr_step_sensor_indic["ind_contratistas_contratos_cancel"] >> [remove_cluster, task_deleted_data]

    [emr_step_sensor_etl["t_seii_ejecucioncon_avancerevses"],
     emr_step_sensor_etl["t_seii_procecotrata_compraadjudi"]] >> \
    emr_step_jobs_indic["ind_contratos_incumplimiento_entregas"] >> emr_step_sensor_indic["ind_contratos_incumplimiento_entregas"] >> [remove_cluster, task_deleted_data]

    [emr_step_sensor_etl["t_seii_multasysanci_secopiimulsa"],
     emr_step_sensor_etl["t_seii_procecotrata_compraadjudi"]] >> \
    emr_step_jobs_indic["ind_inhabilitados_multas"] >> emr_step_sensor_indic["ind_inhabilitados_multas"] >> [remove_cluster, task_deleted_data]

    [emr_step_sensor_etl["t_paco_registro_obras_inconclusa"],
     emr_step_sensor_etl["t_seii_procecotrata_compraadjudi"]] >> \
    emr_step_jobs_indic["ind_inhabilitados_obras_inconclusas"] >> emr_step_sensor_indic["ind_inhabilitados_obras_inconclusas"] >> [remove_cluster, task_deleted_data]

    [emr_step_sensor_etl["t_paco_responsabilidad_fiscales"],
     emr_step_sensor_etl["t_seii_procecotrata_compraadjudi"]] >> \
    emr_step_jobs_indic["ind_inhabilitados_resp_fiscal"] >> emr_step_sensor_indic["ind_inhabilitados_resp_fiscal"] >> [remove_cluster, task_deleted_data]
    

    task_deleted_data >> task_insert_data