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
    dfOfPPro = sources["t_seii_ofertaproces_procesocompr"]
    dfPrCon = sources["t_seii_procecotrata_compraadjudi"]
    date_data = date.today()

    windowSpec = Window.partitionBy("ofertas.id_proceso_compra").orderBy("ofertas.monto_oferta",
                                                                         "ofertas.fecha_registro_oferta")

    dfRespuesta = dfPrCon.alias("contratos").join(dfOfPPro.alias("ofertas"),
                                                  col("id_portafolio") == col("id_proceso_compra"), "inner") \
        .withColumn("tipo_rango_oferta", row_number().over(windowSpec)) \
        .select(col("contratos.nombre_entidad"), col("contratos.id_nit_entidad"), col("contratos.id_proceso"),
                col("contratos.monto_precio_base"), col("contratos.monto_total_adjudicado"),
                col("contratos.nombre_proveedor"), col("contratos.id_nit_proveedor"), col("ofertas.monto_oferta"),
                col("ofertas.nombre_entidad"), col("ofertas.id_nit_entidad"),
                col("ofertas.id_proceso_compra"), col("ofertas.id_nit_proveedor"), col("ofertas.nombre_proveedor"),
                col("tipo_rango_oferta"), col("tipo_estado_resumen")) \
        .select(col("*"), when(col("contratos.nombre_proveedor") == col("ofertas.nombre_proveedor"),
                               lit(1)).otherwise(lit("0")).alias("tipo_nombre_proveedor_igual"),
                when(col("contratos.id_nit_proveedor") == col("ofertas.id_nit_proveedor"), lit(1)).otherwise(
                    lit("0")).alias("tipo_id_nit_proveedor_igual"),
                (col("monto_total_adjudicado") - col("monto_oferta")).alias("monto_diferencia_valor")) \
        .withColumn("tipo_es_mismo_proveedor",
                    when(col("tipo_id_nit_proveedor_igual").isin(1) | col("tipo_nombre_proveedor_igual").isin(1),
                         lit(0)).otherwise(1)) \
        .withColumn("monto_diferencia_valor",
                    when(col("monto_diferencia_valor") < 0, lit(0)).otherwise(col("monto_diferencia_valor"))) \
        .filter(col("tipo_rango_oferta").isin("1"))

    df_result = dfRespuesta.agg(lit("Otros indicadores").alias("nombre_grupo_indicador"),
                                lit("Ofertas Costosas").alias("nombre_indicador"),
                                sum(col("tipo_es_mismo_proveedor")).alias("cantidad_irregularidades"),
                                sum(col("tipo_es_mismo_proveedor")).alias("cantidad_contratos_irregularidades"),
                                sum("monto_diferencia_valor").alias("monto_total_irregularidades"),
                                count("*").alias("cantidad_contratos"),
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
