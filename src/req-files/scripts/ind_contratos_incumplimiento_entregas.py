"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import pytz
import argparse


def get_data_frames(spark, list_source, date_origin):
    dict_sources = {}
    for sources in list_source:
        items = sources.split("=")
        print(f"{items[1]}/fecha_corte_datos={date_origin}")
        dict_sources[items[0]] = spark.read.parquet(f"{items[1]}/fecha_corte_datos={date_origin}")
    return dict_sources


def transform_data(sources, destination_bucket):
    dfEjeCon = sources["t_seii_ejecucioncon_avancerevses"]
    dfPrCon = sources["t_seii_procecotrata_compraadjudi"]
    date_data = datetime.now(pytz.timezone('America/Bogota')).date().isoformat()

    df32PP = dfEjeCon.filter(
        ~col("cant_recibida").isNull() & ~col("cant_recibida").isin("No Definido") & ~col("cant_recibir").isin(
            "No Definido")) \
        .withColumn("tipo_cumple_entrega", when(col("cant_recibida") < col("cant_recibir"), lit(1)).otherwise(lit(0))) \
        .filter(col("tipo_cumple_entrega").isin(1)) \
        .groupBy(col("id_contrato")).count() \
        .withColumn("id_contrato_final", substring("id_contrato", 12, 8))

    df32C = dfPrCon.select(substring("id_proceso", 9, 8).alias("id_contrato_final2"), col("id_proceso"),
                           col("id_nit_proveedor"), col("monto_precio_base"), col("nombre_contrato")) \
        .filter(~col("id_nit_proveedor").isin("No Definido", "No Adjudicado")).distinct()

    df32 = df32PP.join(df32C, col("id_contrato_final") == col("id_contrato_final2"), "inner")

    df_result = df32.agg(
        lit("indicadores incumplimiento").cast("string").alias("nombre_grupo_indicador"),
        lit("contratos con incumplimiento de entregas").cast("string").alias("nombre_indicador"),
        sum(col("count")).cast("long").alias("cantidad_irregularidades"),
        countDistinct(col("id_contrato_final2")).cast("long").alias("cantidad_contratos_irregularidades"),
        sumDistinct("monto_precio_base").cast("decimal(30,3)").alias("monto_total_irregularidades"),
        lit(dfPrCon.count()).cast("long").alias("cantidad_contratos_totales"),
        lit(date_data).cast("date").alias("fecha_ejecucion")
    )

    df_result.write.mode("append") \
        .json(f"s3://{destination_bucket}/t_result_indicadores_batch/fecha_ejecucion={date_data}")
    print(df_result.count())
    logging.info(f"con éxito write data frame t_result_indicadores_batch in {destination_bucket}")


def parse_arguments():
    parser = argparse.ArgumentParser(description='PySpark Job Arguments')
    parser.add_argument('--sources', action='store', type=str, required=True)
    parser.add_argument('--destination_bucket', action='store', type=str, required=True)
    parser.add_argument('--key', action='store', type=str, required=True)
    parser.add_argument('--date_origin', action='store', type=str, required=True)
    parser.add_argument('--app_name', action='store', type=str, required=True)
    args = parser.parse_args()
    return args


def main():
    pyspark_args = parse_arguments()
    spark = SparkSession.builder.appName(pyspark_args.app_name).getOrCreate()
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    transform_data(
        sources=get_data_frames(spark, pyspark_args.sources.split(","), pyspark_args.date_origin),
        destination_bucket=pyspark_args.destination_bucket
    )


if __name__ == '__main__':
    main()
