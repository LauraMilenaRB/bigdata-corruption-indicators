"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import time
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
from datetime import datetime
import pytz
import argparse
import subprocess


def parse_arguments():
    parser = argparse.ArgumentParser(description='PySpark Job Arguments')
    parser.add_argument('--endpoint', action='store', type=str, required=True)
    parser.add_argument('--user', action='store', type=str, required=True)
    parser.add_argument('--pwd', action='store', type=str, required=True)
    parser.add_argument('--db', action='store', type=str, required=True)
    parser.add_argument('--id_cluster', action='store', type=str, required=True)
    args = parser.parse_args()
    return args


def main():
    spark = SparkSession.builder.appName('raw-elasticmapreduce_streaming').getOrCreate()
    spark.sql("set spark.sql.streaming.schemaInference=true")
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    schema = StructType(
        [StructField("event_date", DateType(), True),
         StructField("event_time", StringType(), True),
         StructField("id_no_contrato", StringType(), True),
         StructField("id_nit_entidad", StringType(), True),
         StructField("id_nit_proveedor", StringType(), True),
         StructField("id_nit_empresa", StringType(), True),
         StructField("id_portafolio", StringType(), True),
         StructField("nombre_proveedor", StringType(), True),
         StructField("nombre_responsable_fiscal", StringType(), True),
         StructField("monto_contrato", DecimalType(30, 3), True)
         ])

    pyspark_args = parse_arguments()
    command = f'aws emr add-steps --cluster-id {pyspark_args.id_cluster} ' \
              f'--steps Type=CUSTOM_JAR,Name="spark_stream_ind",Jar="command-runner.jar",ActionOnFailure=CONTINUE,Args=[spark-submit,--master,yarn,--deploy-mode,client,s3://test-pgr-req-files/scripts/spark_stream_ind_mini_batch_loop.py,' \
              f'--endpoint,{pyspark_args.endpoint},--user,{pyspark_args.user},--pwd,{pyspark_args.pwd},--db,{pyspark_args.db}]'
    #subprocess.run(command, shell=True)
    while True:
        date_data = datetime.now(pytz.timezone('America/Bogota')).date().isoformat()
        data_source = spark.read.schema(schema).json(f"s3://test-pgr-staging-zone/t_streaming_contracts/{date_data}/")
        if data_source.count() != 0:
            print(f"Read text s3://test-pgr-staging-zone/t_streaming_contracts/{date_data}/")
            data_source.write.mode("overwrite").parquet(f"s3://test-pgr-raw-zone/t_streaming_contracts/{date_data}")
            subprocess.run(command, shell=True)
        time.sleep(90)


if __name__ == '__main__':
    main()
