import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
from datetime import date


def get_data_frames(spark, list_source, date_origin):
    dict_sources = {}
    for sources in list_source:
        items = sources.split("=")
        print(f"{items[1]}/fecha_corte_datos={date_origin}")
        dict_sources[items[0]] = spark.read.parquet(f"{items[1]}/fecha_corte_datos={date_origin}")
    return dict_sources


def transform_data(sources, destination_bucket):
    dfAbusoContratacion = sources["t_seii_procecotrata_compraadjudi"].select(col("id_nit_entidad"),
                                                                             col("monto_total_adjudicado"),
                                                                             col("id_nit_proveedor")) \
        .filter(~col("id_nit_proveedor").isin("No Definido", "No Adjudicado")) \
        .groupBy(col("id_nit_entidad"), col("id_nit_proveedor")).agg(count("*").alias("cantidad_asignadas"),
                                                                     sum("monto_total_adjudicado").alias(
                                                                         "monto_suma_total_adjudicado")) \
        .orderBy(col("cantidad_asignadas").desc())

    date_data = date.today()

    # Cantidad de irregularidades detectadas
    print("Cantidad de irregularidades detectadas:")
    irregularidades = dfAbusoContratacion.filter(col("cantidad_asignadas") >= 3).count()
    # Total de contratos en la base
    total = dfAbusoContratacion.count()

    df_result = dfAbusoContratacion.filter(col("cantidad_asignadas") >= 3) \
        .agg(lit("Otros indicadores").alias("nombre_grupo_indicador"),
             lit("Abuso de la contratación").alias("nombre_indicador"),
             lit(irregularidades).alias("cantidad_irregularidades"),
             sum("cantidad_asignadas").alias("cantidad_contratos_irregularidades"),
             sum("monto_suma_total_adjudicado").alias("monto_total_irregularidades"),
             lit(total).alias("cantidad_contratos_totales"),
             lit(date_data).cast("date").alias("fecha_ejecucion")
             )

    df_result.write.partitionBy("nombre_grupo_indicador", "nombre_indicador", "fecha_ejecucion").mode("overwrite") \
        .json(f"s3://{destination_bucket}/t_result_indicadores_batch")
    print(df_result.count())
    logging.info(f"Success write data frame t_result_indicadores_batch in {destination_bucket}")


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
