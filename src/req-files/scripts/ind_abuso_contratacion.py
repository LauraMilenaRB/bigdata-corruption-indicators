"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import logging
from datetime import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse


def get_data_frames(spark, list_source, date_origin):
    dict_sources = {}
    for sources in list_source:
        items = sources.split("=")
        print(f"{items[1]}/fecha_corte_datos={date_origin}")
        dict_sources[items[0]] = spark.read.parquet(f"{items[1]}/fecha_corte_datos={date_origin}")
    return dict_sources


def transform_data(sources, destination_bucket):
    dfPrCon = sources["t_seii_procecotrata_compraadjudi"]
    date_data = datetime.now(pytz.timezone('America/Bogota')).date().isoformat()

    dfAbusoContratacion = dfPrCon.dropDuplicates(["id_proceso"]).select(col("id_nit_entidad"), col("monto_precio_base"),
                                                                        col("id_nit_proveedor"),
                                                                        year(col("fecha_publicacion")).alias(
                                                                            "anio_fecha_publicacion")) \
        .filter(~col("id_nit_proveedor").isin("No Definido", "No Adjudicado")) \
        .groupBy(col("id_nit_entidad"), col("id_nit_proveedor"), col("anio_fecha_publicacion")).agg(
        count("*").alias("cantidad_asignadas"), sum("monto_precio_base").alias("monto_suma_total_adjudicado")) \
        .orderBy(col("cantidad_asignadas").desc())



    # Cantidad de irregularidades detectadas
    irregularidades = dfAbusoContratacion.filter(col("cantidad_asignadas") >= 3).count()
    # Total de contratos en la base
    total = dfAbusoContratacion.count()

    df_result = dfAbusoContratacion.filter(col("cantidad_asignadas") >= 3) \
        .agg(
        lit("otros indicadores").cast("string").alias("nombre_grupo_indicador"),
        lit("abuso de la contratación").cast("string").alias("nombre_indicador"),
        lit(irregularidades).cast("long").alias("cantidad_irregularidades"),
        sum("cantidad_asignadas").cast("long").alias("cantidad_contratos_irregularidades"),
        sum("monto_suma_total_adjudicado").cast("decimal(30,3)").alias("monto_total_irregularidades"),
        lit(total).cast("long").alias("cantidad_contratos_totales"),
        lit(date_data).cast("date").alias("fecha_ejecucion")
    )

    df_result.write.mode("append") \
        .json(f"s3://{destination_bucket}/t_result_indicadores_batch/fecha_ejecucion={date_data}")
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
