# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
import boto3

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook

from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor

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
                # with open(f'{key_source}.{data_type_origin}', mode='wb') as localfile:
                # localfile.write(data_type_origin.content).close()
            if s3_hook.check_for_key(bucket_name=bucket_name, key=f'{key_source}/{id_date_load_data}_SUCCESS'):
                s3_hook.delete_objects(bucket=bucket_name, keys=f'{key_source}/{id_date_load_data}_SUCCESS')
            s3_hook.load_string(bucket_name=bucket_name, key=f'{key_source}/{id_date_load_data}_SUCCESS',
                                string_data="SUCCESS")

        return True
    else:
        return False


def steps_etl():
    dict_steps = {}
    if len(endpoint_url_arg) > 0:
        for key_step in endpoint_url_arg.keys():
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
            bucket_key=f'{key}/{id_date_load_data}_SUCCESS'
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
            task_id=f'emr_step_{key}',
            aws_conn_id='aws_default',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
            steps=[step]
        )
        emr_step_jobs_etl[key] = add_emr_spark_step

        emr_spark_job_sensor = EmrStepSensor(
            task_id=f'emr_job_sensor_{key}',
            aws_conn_id='aws_default',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
            step_id=f"{{{{ task_instance.xcom_pull(task_ids='emr_step_{key}')[0] }}}}"
        )
        emr_step_sensor_etl[key] = emr_spark_job_sensor

    steps_inds = steps_ind()

    emr_step_jobs_indic = {}
    emr_step_sensor_indic = {}

    for key, step in steps_inds.items():
        add_emr_spark_step = EmrAddStepsOperator(
            task_id=f'emr_step_{key}',
            aws_conn_id='aws_default',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
            steps=[step]
        )
        emr_step_jobs_indic[key] = add_emr_spark_step

        emr_spark_job_sensor = EmrStepSensor(
            task_id=f'emr_job_sensor_{key}',
            aws_conn_id='aws_default',
            job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
            step_id=f"{{{{ task_instance.xcom_pull(task_ids='emr_step_{key}')[0] }}}}"
        )
        emr_step_sensor_indic[key] = emr_spark_job_sensor

    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}"
    )

    create_table = AthenaOperator(
        task_id="create_table",
        query=DDL_results,
        database=athena_database,
        output_location=f"s3://{output_location_athena}/",
    )

    await_query = AthenaSensor(
        task_id="await_query",
        query_execution_id=create_table.output,
    )

    '''
    create_data_source_athena = boto3.client("quicksight").create_data_source(
        AwsAccountId=boto3.client('sts').get_caller_identity().get("Account"),
        DataSourceId="t_result_indicadores_batch3",
        Name="t_result_indicadores_batch3",
        Type="ATHENA"
    )
    '''


    download_origin_data >> [sensor_staging_bucket for sensor_staging_bucket in
                             check_download_bucket] >> create_emr_cluster

    [create_emr_cluster >> step >> sensor for step, sensor in
     zip(emr_step_jobs_etl.values(), emr_step_sensor_etl.values())]

    emr_step_sensor_etl["t_seii_procecotrata_compraadjudi"] >> emr_step_jobs_indic["ind_abuso_contratacion"] >> \
    emr_step_sensor_indic["ind_abuso_contratacion"] >> [remove_cluster, create_table]

    [emr_step_sensor_etl["t_seii_procecotrata_compraadjudi"],
     emr_step_sensor_etl["t_seii_ofertaproces_procesocompr"]] >> \
    emr_step_jobs_indic["ind_ofertas_costosas"] >> emr_step_sensor_indic["ind_ofertas_costosas"] >> [remove_cluster, create_table]

    [remove_cluster, create_table] >> await_query
