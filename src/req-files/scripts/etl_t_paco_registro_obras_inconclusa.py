"""
Autores: Laura Milena Ramos Bermúdez y Juan Pablo Arevalo Merchán
laura.ramos-b@mail.escuelaing.edu.co
juan.arevalo-m@mail.escuelaing.edu.co
"""

import logging

import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
from datetime import date, datetime


def transform_data(spark, key, date_origin, source_bucket, destination_bucket):
    #data_source = spark.read.option("multiline", "true").json(f"s3://{source_bucket}/{key}/{key}_{date_origin}.json")

    data_source = spark.read.option("header", True).csv(f"s3://{source_bucket}/origen/obras inconclusas.csv")

    date_data = datetime.now(pytz.timezone('America/Bogota')).date().isoformat()
    data_transform = data_source.select(
        trim(col("_c0")).alias("numero_registro"),
        trim(col("COD_ENTIDAD")).alias("id_entidad"),
        trim(col("COD_OBRA")).alias("id_obra"),
        trim(col("CONSECUTIVO")).alias("id_consecutivo"),
        trim(col("NUMERO_ACTA_2011")).alias("id_acta"),
        to_date(col("FECHA_2011"), "yyyy-mm-dd").alias("fecha_acta"),
        trim(col("COMPROMISOS_2011")).alias("desc_compromiso"),
        trim(col("ENTIDAD_2010")).alias("id_nit_entidad"),
        trim(col("NUMERO_PROCESO_2010")).alias("id_proceso"),
        trim(col("TIPO_2010")).alias("tipo_obra_inconclusa"),
        trim(col("ESTADO_2010")).alias("tipo_estado_contrato"),
        trim(col("NIT_ASEGURADORA_2009")).alias("id_nit_aseguradora"),
        trim(col("NOMBRE_ASEGURADORA_2009")).alias("nombre_aseguradora"),
        to_date(col("FECHA_INICIO_2009"), "yyyy-mm-dd").alias("fecha_incio_asegurar"),
        to_date(col("FECHA_FIN_2009"), "yyyy-mm-dd").alias("fecha_fin_asegurar"),
        trim(col("NUMERO_POLIZA_2009")).alias("id_poliza_seguro"),
        trim(col("VALOR_ASEGURADO_2009")).cast("decimal(30,3)").alias("monto_asegurado"),
        trim(col("TIPO_GARANTIA_2009")).alias("tipo_marca_garantia"),
        trim(col("RIESGOS_2009")).alias("desc_riesgos_obra"),
        trim(col("INDEMNIZACION_2009")).alias("tipo_marca_indemnizacion"),
        trim(col("VALOR_INDEMNIZADO_2009")).cast("decimal(30,3)").alias("monto_indemnizado"),
        trim(col("VALOR_2008")).cast("decimal(30,3)").alias("monto_obra"),
        to_date(col("FECHA_2008"), "yyyy-mm-dd").alias("fecha_indemnizacion"),
        trim(col("ESTADO_PRESUPUESTO_2008")).alias("tipo_marca_estado_presupuesto"),
        trim(col("DESCRIPCION_2007")).alias("desc_obra_inconclusa"),
        to_date(col("FECHA_2007"), "yyyy-mm-dd").alias("fecha_licencia"),
        trim(col("AUTORIDAD_2007")).alias("id_nit_autoridad"),
        trim(col("NUMERO_LICENCIA_2007")).alias("id_licencia_construccion"),
        trim(col("ROL_2006")).alias("nombre_rol"),
        trim(col("TIPO_IDENTIFICACION_2006")).alias("tipo_identificacion"),
        trim(col("IDENTIFICACION_2006")).alias("id_rol"),
        trim(col("NOMBRE_2006")).alias("nombre_rol_1"),
        trim(col("FUENTE_2005")).alias("desc_origen_recuros"),
        trim(col("PORCENTAJE_2005")).alias("por_origen_recursos"),
        trim(col("COD_DEPARTAMENTO_2004")).cast("Integer").cast("String").alias("id_departamento"),
        trim(col("DEPARTAMENTO_2004")).alias("nombre_departamento"),
        trim(col("COD_CIUDAD_2004")).cast("Integer").cast("String").alias("id_ciudad"),
        trim(col("CIUDAD_2004")).alias("nombre_ciudad"),
        to_date(col("FECHA_INICIO_2002"), "yyyy-mm-dd").alias("fecha_modificacion"),
        trim(col("OTROSI_2002")).alias("tipo_otro_si"),
        trim(col("TIPO_MODIFICACION_2002")).alias("tipo_marca_modificacion"),
        trim(col("NUMERO_CONTRATO_2002")).alias("id_contrato"),
        trim(col("JUSTIFICACION_2002")).alias("desc_justificacion_mod"),
        trim(col("OBJETO_2002")).alias("desc_objeto_contrato"),
        trim(col("VALOR_CONTRATO_2002")).cast("decimal(30,3)").alias("monto_contrato"),
        trim(col("PLAZO_EN_DIAS_2002")).cast("Integer").alias("numero_dias_plazo"),
        trim(col("ESTA_SECOP_2002")).alias("tipo_marca_secop"),
        trim(col("CODIGO_SECOP_2002")).alias("id_secop"),
        trim(col("DESCRIPCION_2000")).alias("desc_contrato"),
        trim(col("COD_SECTOR_2000")).cast("Integer").cast("String").alias("id_sector"),
        trim(col("SECTOR_2000")).alias("nombre_sector"),
        trim(col("GRUPO_2000")).alias("nombre_grupo"),
        trim(col("NOMBREENTIDAD_2000")).alias("nombre_entidad"),
        trim(col("AREA_PREDIO_2000")).cast("decimal(10,5)").alias("numero_area_predio"),
        trim(col("AREA_CONTRATADA_2000")).cast("decimal(10,5)").alias("numero_area_contratada"),
        trim(col("AREA_CONSTRUIDA_2000")).cast("decimal(10,5)").alias("numero_area_construida"),
        trim(col("ULTIMO_AVANCE_2000")).cast("decimal(7,4)").alias("por_ultimo_avance"),
        trim(col("PRESUPUESTO_TOTAL_2000")).cast("decimal(30,3)").alias("monto_presupuesto_total"),
        trim(col("MATRICULA_INMOBILIARIA_2000")).alias("id_matricula_inmobiliaria"),
        trim(col("CEDULA_CATASTRAL_2000")).alias("id_catastral"),
        trim(col("PORQUE_ESTADO_2000")).alias("desc_motivo_estado"),
        trim(col("DECISION_ADMIN_2000")).alias("desc_decision_administrativo"),
        trim(col("ACTO_ADMINISTRATIVO_2000")).alias("desc_acto_administrativo"),
        trim(col("CLASE_OBRA_2000")).alias("tipo_obra"),
        trim(col("ESTADO_REGISTRO_2000")).alias("tipo_marca_registro"),
        trim(col("ORIGEN_REGISTRO_2000")).alias("nombre_origen_registro"),
        trim(col("TIPO_REPORTE_2000")).alias("desc_reporte_obra"),
        trim(col("RAZ_NOUSO_2000")).alias("desc_motivo_no_uso"),
        trim(col("LONGITUDINICIAL_2000")).cast("decimal(10,5)").alias("numero_longitud_inicial"),
        trim(col("LONGITUDFINAL_2000")).cast("decimal(10,5)").alias("numero_longitud_final"),
        trim(col("LATITUDINICIAL_2000")).cast("decimal(10,5)").alias("numero_latitud_inicial"),
        trim(col("LATITUDFINAL_2000")).cast("decimal(10,5)").alias("numero_latitud_final"),
        trim(col("PR_INICIAL_2000")).alias("campo_libre"),
        trim(col("PR_INICIAL_M_2000")).alias("campo_libre_1"),
        trim(col("PR_FINAL_2000")).alias("campo_libre_2"),
        trim(col("PR_FINAL_M_2000")).alias("campo_libre_3"),
        trim(col("NIT_2001")).alias("id_nit_entidad_1"),
        trim(col("ENTIDAD_2001")).alias("nombre_entidad_1"),
        trim(col("DESCRIPCION_2003")).alias("desc_actualizacion_valor"),
        trim(col("PRESUPUESTO_TOTAL_2000")).cast("decimal(30,3)").alias("monto_presupuesto_total_1"),
        trim(col("VALOR_2003")).cast("decimal(30,3)").alias("monto_contrato_1"),
        to_date(col("FECHA_2003"), "yyyy-mm-dd").alias("fecha_registro_valor"),
        lit(date_data).cast("date").alias("fecha_corte_datos")
    )\
        .withColumn("id_nit_entidad", regexp_replace(col("id_nit_entidad"),"[A-Z_a-z\/.,*\\t� -]",""))\
        .withColumn("id_nit_entidad_1", regexp_replace(col("id_nit_entidad_1"),"[A-Z_a-z\/.,*\\t� -]",""))\

    data_transform.write.mode('overwrite').partitionBy("fecha_corte_datos").parquet(f"s3://{destination_bucket}/{key}")
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
