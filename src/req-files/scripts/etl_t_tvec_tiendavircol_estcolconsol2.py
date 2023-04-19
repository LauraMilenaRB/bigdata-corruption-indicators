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
    data_source = spark.read.option("multiline", "true").json(f"s3://{source_bucket}/{key}/{key}_{date_origin}.json")

    date_data = date.today()
    data_transform = data_source.select(
        trim(col("a_o")).alias("fecha_anio_orden_compra"),
        trim(col("identificador_de_la_orden")).alias("id_orden_compra"),
        trim(col("rama_de_la_entidad")).alias("nombre_rama_entidad"),
        trim(col("sector_de_la_entidad")).alias("nombre_sector_entidad"),
        trim(col("entidad")).alias("nombre_entidad"),
        upper(trim(col("solicitante"))).alias("nombre_solicitante"),
        to_timestamp(trim(col("fecha")), "yyyy-mm-dd'T'HH:mm:ss").alias("fecha_orden_compra"),
        upper(trim(col("proveedor"))).alias("nombre_proveedor"),
        when(trim(col("estado")).isNull(), "NoDefinido").otherwise(trim(col("Estado"))).alias("tipo_estado_orden"),
        trim(col("solicitud")).alias("id_solicitud_compra"),
        trim(col("items")).alias("desc_items_orden_compra"),
        trim(col("total")).cast("decimal(30,3)").alias("monto_total_orden_compra"),
        trim(col("agregacion")).alias("tipo_agregacion_orden_compra"),
        trim(col("ciudad")).alias("nombre_ciudad_orden"),
        trim(col("entidad_obigada")).alias("tipo_entidad_obligada"),
        trim(col("espostconflicto")).alias("tipo_marca_postconflicto"),
        trim(col("nit_proveedor")).alias("id_nit_proveedor"),
        trim(col("actividad_economica_proveedor")).alias("id_actividad_economica"),
        trim(col("id_entidad")).alias("id_entidad"),
        lit(date_data).cast("date").alias("fecha_corte_datos")
    )

    data_transform.write.mode('overwrite').partitionBy("fecha_corte_datos", "tipo_estado_orden").parquet(
        f"s3://{destination_bucket}/{key}")
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
