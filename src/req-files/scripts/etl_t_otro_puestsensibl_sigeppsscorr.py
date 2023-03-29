import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
from datetime import date


def transform_data(spark, key, date_origin, source_bucket, destination_bucket):
    #data_source = spark.read.option("multiline", "true").json(f"s3://{source_bucket}/{key}/{key}_{date_origin}.json")
    data_source = spark.read.option("header", True).csv(f"s3://{source_bucket}/origen/Puestos_Sensibles_a_la_Corrupci_n.csv")

    date_data = date.today()
    data_transform = data_source.select(
        trim(col("TIPO_DCTO")).alias("tipo_identificacion"),
        trim(col("IDENTIFICACION_FUNCIONARIO")).alias("id_nit_identificacion"),
        upper(trim(col("NOMBRE_COMPLETO"))).alias("nombre_empleado"),
        trim(col("SEXO")).alias("desc_sexo_empleado"),
        trim(col("NACIONALIDAD")).alias("nombre_nacionalidad_empleado"),
        trim(col("MUNICIPIO_NACIMIENTO")).alias("nombre_mcpio_empleado"),
        trim(col("DPTO_NACIMIENTO")).alias("nombre_depto_empleado"),
        trim(col("MESES_EXPERIENCIA_PUBLICO")).alias("numero_meses_exp_pub"),
        trim(col("MESES_EXPERIENCIA_PRIVADO")).alias("numero_meses_exp_priv"),
        trim(col("MESES_EXP_NEG_PROPIO")).alias("numero_meses_exp_ind"),
        trim(col("MESES_EXPERIENCIA_DOCENTE")).alias("numero_meses_exp_doc"),
        trim(col("NIVEL_ACADEMICO")).alias("tipo_nivel_educativo"),
        trim(col("NIVEL_FORMACION")).alias("tipo_nivel_formacion"),
        trim(col("COD_INSTITUCION")).alias("id_entidad"),
        trim(col("NOMBRE_INSTITUCION")).alias("nombre_entidad"),
        trim(col("MUNICIPIO_INSTITUCION")).alias("nombre_mcpio_entidad"),
        trim(col("DPTO_INSTITUCION")).alias("nombre_depto_entidad"),
        trim(col("ORDEN")).alias("tipo_orden_territorial_entidad"),
        trim(col("SECTOR_ADMTIVO")).alias("tipo_sector_admin"),
        trim(col("NATURALEZA_JURIDICA")).alias("tipo_naturaleza"),
        trim(col("CLASIFICACION_ORGANICA")).alias("nombre_rama_entidad"),
        trim(col("NIVEL_JERARQUICO_EMPLEO")).alias("tipo_nivel_jerarquico"),
        when(trim(col("TIPO_NOMBRAMIENTO")).isNull(), "NO DEFINIDO").otherwise(trim(col("TIPO_NOMBRAMIENTO"))).alias(
            "tipo_nombramiento"),
        trim(col("DENOMINACION_EMPLEO_ACTUAL")).alias("nombre_cargo"),
        trim(col("DEPENDENCIA_EMPLEO_ACTUAL")).alias("tipo_dependencia"),
        regexp_replace("ASIG_BASICA", ",", "").cast("decimal(30,3)").alias("monto_asignacion_salarial"),
        to_timestamp(trim(col("FECHA_VINCULACION")), "MM/dd/yyyy HH:mm:ss").alias("fecha_vinculacion"),
        lit(date_data).cast("date").alias("fecha_corte_datos")
    )

    data_transform.write.mode('overwrite').partitionBy("fecha_corte_datos", "tipo_nombramiento").parquet(f"s3://{destination_bucket}/{key}")
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
