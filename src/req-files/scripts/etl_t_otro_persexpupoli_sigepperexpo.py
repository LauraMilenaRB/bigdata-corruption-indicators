"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""


import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
from datetime import date


def transform_data(spark, key, date_origin, source_bucket, destination_bucket):
    #data_source = spark.read.option("multiline", "true").json(f"s3://{source_bucket}/{key}/{key}_{date_origin}.json")
    data_source = spark.read.option("header", True).csv(f"s3://{source_bucket}/origen/Personas_Expuestas_Pol_ticamente__PEP_.csv")

    date_data = date.today()
    data_transform = data_source.select(
        trim(col("NUMERO_DOCUMENTO")).alias("id_nit_pep"),
        upper(trim(col("NOMBRE_PEP"))).alias("nombre_pep"),
        trim(col("DENOMINACION_CARGO")).alias("nombre_cargo_pep"),
        trim(col("NOMBRE_ENTIDAD")).alias("nombre_entidad"),
        to_timestamp(trim(col("FECHA_VINCULACION")), "MM/dd/yyyy HH:mm:ss").alias("fecha_vinculacion"),
        to_timestamp(trim(col("FECHA_DESVINCULACION")), "MM/dd/yyyy HH:mm:ss").alias("fecha_desvinculacion"),
        trim(col("ENLACE_HOJA_VIDA_SIGEP")).alias("desc_url_hoja_vida_pep"),
        lit(date_data).cast("date").alias("fecha_corte_datos")
    )

    data_transform.write.mode('overwrite').partitionBy("fecha_corte_datos").parquet(f"s3://{destination_bucket}/{key}")
    logging.info(f"con éxito write data frame {key} in {destination_bucket}")


def parse_arguments():
    parser = argparse.ArgumentParser(description='PySpark Job Arguments')
    parser.add_argument('--staging_bucket', action='store', type=str, required=True)
    parser.add_argument('--raw_bucket', action='store', type=str, required=True)
    parser.add_argument('--key', action='store', type=str, required=True)
    parser.add_argument('--date_origin', action='store', type=str, required=True)
    parser.add_argument('--app_name', action='store', type=str, required=True)
    args = parser.parse_args()
    return args


def main():
    pyspark_args = parse_arguments()
    print(pyspark_args.staging_bucket, pyspark_args.raw_bucket, pyspark_args.app_name)
    spark = SparkSession.builder.appName(pyspark_args.app_name).getOrCreate()
    transform_data(spark, key=pyspark_args.key, date_origin=pyspark_args.date_origin,
                   source_bucket=pyspark_args.staging_bucket,
                   destination_bucket=pyspark_args.raw_bucket
                   )


if __name__ == '__main__':
    main()
