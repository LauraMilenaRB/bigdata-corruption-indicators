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
    date_data = datetime.now(pytz.timezone('America/Bogota')).date().isoformat()
    data_transform = data_source.select(
        to_date(trim(col("fecha_de_registro")), "yyyy-mm-dd").alias("fecha_registro_oferta"),
        trim(col("referencia_de_la_oferta")).alias("desc_oferta"),
        trim(col("identificador_de_la_oferta")).alias("id_oferta"),
        trim(col("valor_de_la_oferta")).cast("decimal(30,3)").alias("monto_oferta"),
        trim(col("entidad_compradora")).alias("nombre_entidad"),
        trim(col("nit_entidad_compradora")).alias("id_nit_entidad"),
        trim(col("moneda")).alias("desc_moneda_monto"),
        trim(col("descripcion_del_procedimiento")).alias("desc_contrato"),
        trim(col("referencia_del_proceso")).alias("id_referencia"),
        trim(col("id_del_proceso_de_compra")).alias("id_proceso_compra"),
        trim(col("modalidad")).alias("tipo_contratacion"),
        trim(col("invitacion_directa")).alias("tipo_invitacion_directa"),
        upper(trim(col("nombre_proveedor"))).alias("nombre_proveedor"),
        trim(col("nit_del_proveedor")).alias("id_nit_proveedor"),
        trim(col("codigo_entidad")).alias("id_entidad"),
        trim(col("codigo_proveedor")).alias("id_proveedor_secop"),
        lit(date_data).cast("date").alias("fecha_corte_datos")
    )

    data_transform.write.mode('overwrite').partitionBy("fecha_corte_datos", "tipo_contratacion").parquet(f"s3://{destination_bucket}/{key}")
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
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    transform_data(spark, key=pyspark_args.key, date_origin=pyspark_args.date_origin,
                   source_bucket=pyspark_args.staging_bucket,
                   destination_bucket=pyspark_args.raw_bucket
                   )


if __name__ == '__main__':
    main()
