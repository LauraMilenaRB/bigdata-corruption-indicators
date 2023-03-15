import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
from datetime import date


def transform_data(spark, key, date_origin, source_bucket, destination_bucket):
    data_source = spark.read.option("multiline", "true").json(f"s3://{source_bucket}/{key}/{key}_{date_origin}.json")

    date_data = date.today()
    data_transform = data_source.select(
        trim(col("codigo_camara")).alias("id_camara"),
        upper(trim(col("camara_comercio"))).alias("nombre_camara"),
        trim(col("matricula")).alias("id_matricula_mercantil"),
        upper(trim(col("inscripcion_proponente"))).alias("id_inscripcion_proponente"),
        upper(trim(col("razon_social"))).alias("nombre_razon_social"),
        upper(trim(col("primer_apellido"))).alias("nombre_primer_apellido"),
        upper(trim(col("segundo_apellido"))).alias("nombre_segundo_apellido"),
        upper(trim(col("primer_nombre"))).alias("nombre_primero"),
        upper(trim(col("segundo_nombre"))).alias("nombre_segundo"),
        upper(trim(col("sigla"))).alias("nombre_sigla"),
        trim(col("codigo_clase_identificacion")).alias("codigo_clase_identificacion"),
        trim(col("clase_identificacion")).alias("tipo_identificacion"),
        trim(col("numero_identificacion")).alias("id_empresa"),
        trim(col("nit")).alias("id_nit_empresa"),
        trim(col("digito_verificacion")).alias("id_digito_verificacion"),
        trim(col("cod_ciiu_act_econ_pri")).alias("id_ciiu_principal"),
        trim(col("cod_ciiu_act_econ_sec")).alias("id_ciiu_secundaria"),
        trim(col("ciiu3")).alias("id_ciiu_terciaria"),
        trim(col("ciiu4")).alias("id_ciiu_cuaternaria"),
        to_date(trim(col("fecha_matricula")), "yyyyMMdd").alias("fecha_matricula"),
        to_date(trim(col("fecha_renovacion")), "yyyyMMdd").alias("fecha_ultima_renovacion"),
        trim(col("ultimo_ano_renovado")).alias("fecha_anio_renovado"),
        to_date(trim(col("fecha_vigencia")), "yyyyMMdd").alias("fecha_vigencia"),
        to_date(trim(col("fecha_cancelacion")), "yyyyMMdd").alias("fecha_cancelacion"),
        trim(col("codigo_tipo_sociedad")).alias("codigo_tipo_sociedad"),
        trim(col("tipo_sociedad")).alias("tipo_sociedad"),
        trim(col("codigo_organizacion_juridica")).alias("codigo_organziacion"),
        trim(col("organizacion_juridica")).alias("tipo_organizacion"),
        trim(col("codigo_categoria_matricula")).alias("codigo_categoria_matricula"),
        trim(col("categoria_matricula")).alias("categoria_matricula"),
        trim(col("codigo_estado_matricula")).alias("codigo_estado_matricula"),
        trim(col("estado_matricula")).alias("tipo_estado_matricula"),
        trim(col("clase_identificacion_rl")).alias("tipo_identificacion_rep_leg"),
        trim(col("num_identificacion_representante_legal")).alias("id_nit_rep_leg"),
        upper(trim(col("representante_legal"))).alias("nombre_rep_leg"),
        to_timestamp(trim(col("fecha_actualizacion")), "yyyy/MM/dd HH:mm:ss").alias("fecha_acutalizacion"),
        lit(date_data).cast("date").alias("fecha_corte_datos")
    ).withColumn("fecha_matricula", when(year("fecha_matricula") < 1959, lit(None)).otherwise(col("fecha_matricula"))) \
        .withColumn("fecha_ultima_renovacion",
                    when(year("fecha_ultima_renovacion") < 1959, lit(None)).otherwise(col("fecha_ultima_renovacion"))) \
        .withColumn("fecha_vigencia", when(year("fecha_vigencia") < 1959, lit(None)).otherwise(col("fecha_vigencia"))) \
        .withColumn("fecha_cancelacion",
                    when(year("fecha_cancelacion") < 1959, lit(None)).otherwise(col("fecha_cancelacion")))

    data_transform.write.mode('overwrite').partitionBy("fecha_corte_datos", "tipo_estado_matricula").parquet(f"s3://{destination_bucket}/{key}")
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
