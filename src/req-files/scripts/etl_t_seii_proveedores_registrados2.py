import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
from datetime import date


def transform_data(spark, key, date_origin, source_bucket, destination_bucket):
    data_source = spark.read.option("multiline", "true").json(f"s3://{source_bucket}/{key}/{key}_{date_origin}.json")

    date_data = date.today()
    data_transform = data_source.select(
        trim(col("ID Proceso")).alias("id_proceso"),
        trim(col("Referencia Proceso")).alias("id_referencia"),
        trim(col("ID Contrato")).alias("id_contrato"),
        trim(col("Codigo Entidad Creadora")).alias("id_entidad"),
        upper(trim(col("Nombre Entidad Creadora"))).alias("nombre_entidad"),
        trim(col(" Codigo Proveedor Objeto de la Multa")).alias("id_contratista"),
        upper(trim(col("Nombre Proveedor Objeto de la Multa"))).alias("nombre_contratista_sancionado"),
        trim(col("Valor")).cast("decimal(30,3)").alias("monto_sancion"),
        trim(col("Valor Pagado")).cast("decimal(30,3)").alias("monto_sancion_pagado"),
        to_timestamp(trim(col("Fecha Evento")), "MM/dd/yyyy HH:mm:ss").alias("fecha_registro_sanacion"),
        trim(col("Aplico Garantias")).alias("tipo_aplica_garantia"),
        trim(col("Numero de Acto")).alias("codigo_acto"),
        trim(col("Tipo de Sancion")).alias("tipo_sancion"),
        trim(col("Descripcion Otro Tipo De Sancion")).alias("desc_otro_sancion"),
        trim(col("Estado")).alias("tipo_estado"),
        trim(col("Tipo")).alias("tipo_multa"),
        trim(col("Numero de Version")).alias("numero_version"),
        lit(date_data).cast("date").alias("fecha_corte_datos")
    )

    data_transform.write.mode('overwrite').partitionBy("fecha_corte_datos").parquet(f"s3://{destination_bucket}/{key}")
    logging.info(f"Success write data frame {key} in {destination_bucket}")


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
