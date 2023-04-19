import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
from datetime import date


def transform_data(spark, key, date_origin, source_bucket, destination_bucket):
    #data_source = spark.read.option("multiline", "true").json(f"s3://{source_bucket}/{key}/{key}_{date_origin}.json")

    data_source = spark.read.option("header",True).csv(f"s3://{source_bucket}/origen/SECOP_II_-_Ejecuci_n_Contratos.csv")

    date_data = date.today()
    data_transform = data_source.select(
        trim(col("NombrePlan")).alias("nombre_plan"),
        trim(col("IdentificadorContrato")).alias("id_contrato"),
        trim(col("TipoEjecucion")).alias("tipo_ejecucion"),
        trim(col("PorcentajeDeAvanceEsperado")).cast("decimal(7,4)").alias("porc_avance_esper"),
        to_timestamp(trim(col("FechaDeEntregaReal")), "yyyy-MM-dd HH:mm:ss.SSSSSSS").alias("fecha_entrega_real"),
        to_timestamp(trim(col("FechaDeEntregaEsperada")), "yyyy-MM-dd HH:mm:ss.SSSSSSS").alias(
            "fecha_entrega_esperada"),
        trim(col("CantidadPorRecibir")).alias("cant_recibir"),
        trim(col("CantidadRecibida")).alias("cant_recibida"),
        to_timestamp(trim(col("FechaCreacion")), "yyyy-MM-dd HH:mm:ss.SSSSSSS").alias("fecha_creacion"),
        lit(date_data).cast("date").alias("fecha_corte_datos")
    )\
        .withColumn("fecha_entrega_real", when(year("fecha_entrega_real").isin("18", "202", "218", "219", "221"),
                                               lit(None)).otherwise(col("fecha_entrega_real")))

    data_transform.write.mode('overwrite').partitionBy("fecha_corte_datos", "tipo_ejecucion").parquet(f"s3://{destination_bucket}/{key}")
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
