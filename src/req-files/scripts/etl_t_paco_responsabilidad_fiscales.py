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

    data_source = spark.read.option("header", True).csv(f"s3://{source_bucket}/origen/responsabilidades_fiscales.csv")

    date_data = date.today()
    data_transform = data_source.select(
        col("Responsable Fiscal").alias("nombre_responsable_fiscal"),
        col("Tipo y Num Docuemento").alias("id_responsable_fiscal"),
        col("Entidad Afectada").alias("nombre_entidad_afectada"),
        col("TR").alias("tipo_respons_responsable_fiscal"),
        col("R").cast("integer").alias("cant_registros_responsable_fiscal"),
        col("Ente que Reporta").alias("desc_apertura_sancion_colusion"),
        col("Departamento").alias("nombre_depto_entidad"),
        col("Municipio").alias("nombre_mcpio_entidad"),
        lit(date_data).cast("date").alias("fecha_corte_datos")
    ).filter(col("R").isNull())

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
