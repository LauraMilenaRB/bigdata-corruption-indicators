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
    spark = SparkSession.builder.appName('raw-stream').getOrCreate()
    spark.sql("set spark.sql.streaming.schemaInference=true")
    date_data = datetime.now(pytz.timezone('America/Bogota')).date().isoformat()
    schema = StructType(
        [StructField("event_date", DateType(), True),
         StructField("event_time", StringType(), True),
         StructField("id_nit_entidad", StringType(), True),
         StructField("id_nit_proveedor", StringType(), True),
         StructField("id_no_contrato", StringType(), True),
         StructField("id_portafolio", StringType(), True),
         StructField("monto_contrato", DecimalType(30, 3), True),
         StructField("nombre_proveedor", StringType(), True),
         StructField("nombre_responsable_fiscal", StringType(), True)
         ])
    data_source = spark.readStream.schema(schema).json(f"s3://test-pgr-staging-zone/t_streaming_contracts/{date_data}/")
    print(f"Read text s3://test-pgr-staging-zone/t_streaming_contracts/{date_data}/")
    pyspark_args = parse_arguments()

    query = data_source \
        .writeStream \
        .outputMode("append") \
        .format(f"parquet") \
        .option("checkpointLocation", f"s3://test-pgr-aws-logs/streams_checkpoints/raw/") \
        .option("path", f"s3://test-pgr-raw-zone/t_streaming_contracts/{date_data}") \
        .start()

    time.sleep(5)

    command = f'aws emr add-steps --cluster-id {pyspark_args.id_cluster} --steps Type=CUSTOM_JAR,Name="Spark Program",Jar="command-runner.jar",ActionOnFailure=CONTINUE,' \
              f'Args=[spark-submit, --master, yarn,--deploy-mode, client, s3://test-pgr-req-files/scripts/spark_stream_ind_mini_batch.py,' \
              f'--endpoint,{pyspark_args.endpoint}, --user,{pyspark_args.user}, ' \
              f'--pwd,{pyspark_args.pwd}, --db,{pyspark_args.db}, --id_cluster,{pyspark_args.id_cluster}]'

    subprocess.run(command, shell=True)

    query.awaitTermination()


if __name__ == '__main__':
    main()
