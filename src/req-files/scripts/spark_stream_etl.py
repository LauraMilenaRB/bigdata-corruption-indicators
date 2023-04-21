"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType
from datetime import datetime
import pytz


def main():
    spark = SparkSession.builder.config("spark.streaming.concurrentJobs", "8").appName('raw-elasticmapreduce_streaming').getOrCreate()
    spark.sql("set spark.sql.streaming.schemaInference=true")
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    date_data = datetime.now(pytz.timezone('America/Bogota')).date().isoformat()
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
    data_source = spark.readStream.schema(schema).json(f"s3://test-pgr-staging-zone/t_streaming_contracts/{date_data}/")
    print(f"Read text s3://test-pgr-staging-zone/t_streaming_contracts/{date_data}/")

    query = data_source \
        .writeStream \
        .outputMode("append") \
        .format(f"parquet") \
        .option("checkpointLocation", f"s3://test-pgr-aws-logs/streams_checkpoints/raw/") \
        .option("path", f"s3://test-pgr-raw-zone/t_streaming_contracts/{date_data}") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    main()
