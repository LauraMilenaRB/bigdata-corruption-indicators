import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
from datetime import date


def transform_data(spark, key, date_origin, source_bucket, destination_bucket):
    #data_source = spark.read.option("multiline", "true").json(f"s3://{source_bucket}/{key}/{key}_{date_origin}.json")
    data_source = spark.read.option("header", True).csv(f"s3://{source_bucket}/origen/SECOP_II_-_Proveedores_Registrados.csv")

    date_data = date.today()
    data_transform = data_source.select(
        trim(col("Nombre")).alias("nombre_proveedor"),
        trim(col("NIT")).alias("id_proveedor"),
        trim(col("Tipo Empresa")).alias("tipo_empresa"),
        trim(col("EsPyme")).alias("tipo_marca_pyme"),
        trim(col("Ubicación")).alias("desc_ubicacion"),
        to_date(col("Fecha Creación"), "MM/dd/yyyy").alias("fecha_creacion"),
        lit(year(fechaT)).alias("fecha_anio_creacion"),
        trim(col("Pais")).alias("nombre_pais"),
        trim(col("Departamento")).alias("nombre_depto_proveedor"),
        trim(col("Municipio")).alias("nombre_mcpio_proveedor"),
        trim(col("Codigo Categoria Principal")).alias("id_unspsc_proveedor"),
        trim(col("Descripcion Categoria Principal")).alias("desc_unspsc_proveedor"),
        trim(col("Tipo Doc Representante Legal")).alias("tipo_identificacion_rep_legal"),
        trim(col("Num Doc Representante legal")).alias("id_representante_legal"),
        trim(col("Nombre Representante Legal")).alias("nombre_representante_legal"),
        trim(col("Codigo")).alias("id_registro_provedor"),
        lit(date_data).cast("date").alias("fecha_corte_datos")
    )

    data_transform.write.mode('overwrite').partitionBy("fecha_corte_datos","fecha_anio_creacion").parquet(f"s3://{destination_bucket}/{key}")
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
