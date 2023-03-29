import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
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
    dfPNPJESAL = sources["t_otro_pernajuesadl_camarcomerci"]
    dfPrCon = sources["t_seii_procecotrata_compraadjudi"]
    date_data = date.today()

    dfPNPJESALCancel = dfPNPJESAL.select(col("nombre_razon_social"), col("tipo_identificacion"), col("id_empresa"),
                                         col("id_nit_empresa"), col("id_digito_verificacion"),
                                         col("fecha_ultima_renovacion"), col("fecha_cancelacion"),
                                         col("tipo_estado_matricula")).filter(
        col("tipo_estado_matricula").isin("CANCELADA"))

    dfPNPJESALSinCan = dfPNPJESAL.select(col("nombre_razon_social"), col("tipo_identificacion"), col("id_empresa"),
                                         col("id_nit_empresa"), col("id_digito_verificacion"),
                                         col("fecha_ultima_renovacion"), col("fecha_cancelacion"),
                                         col("tipo_estado_matricula")).filter(
        col("tipo_estado_matricula").isin("CANCELADA"))

    dfunion1 = dfPrCon.alias("contratos").join(dfPNPJESALCancel.alias("camara"),
                                               col("id_nit_proveedor") == col("id_nit_empresa"), "inner")
    dfunion2 = dfPrCon.alias("contratos").join(dfPNPJESALCancel.alias("camara"),
                                               col("id_nit_proveedor") == col("id_empresa"), "inner")
    dfTotalCan = dfunion1.union(dfunion2).distinct()

    dfunion1SC = dfPrCon.alias("contratos").join(dfPNPJESALSinCan.alias("camara"),
                                                 col("id_nit_proveedor") == col("id_nit_empresa"), "inner")
    dfunion2SC = dfPrCon.alias("contratos").join(dfPNPJESALSinCan.alias("camara"),
                                                 col("id_nit_proveedor") == col("id_empresa"), "inner")
    dfTotalSC = dfunion1SC.union(dfunion2SC).count()

    df_result = dfTotalCan.agg(
        lit("otros indicadores").cast("string").alias("nombre_grupo_indicador"),
        lit("contratos con proveedores inactivos").cast("string").alias("nombre_indicador"),
        count(col("*")).cast("long").alias("cantidad_irregularidades"),
        count(col("*")).cast("long").alias("cantidad_contratos_irregularidades"),
        sum("monto_total_adjudicado").cast("decimal(30,3)").alias("monto_total_irregularidades"),
        lit(dfTotalSC).cast("long").alias("cantidad_contratos"),
        lit(date_data).cast("date").alias("fecha_ejecucion")
    )

    df_result.write.partitionBy("fecha_ejecucion").mode("append") \
        .parquet(f"s3://{destination_bucket}/t_result_indicadores_batch")
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