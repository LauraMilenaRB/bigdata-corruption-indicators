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
    data_source = spark.read.option("header", True).csv(f"s3://{source_bucket}/origen/SECOPII_-_Contratos_Cancelados.csv")

    date_data = datetime.now(pytz.timezone('America/Bogota')).date().isoformat()
    data_transform = data_source.select(
        trim(col("Nombre Entidad")).alias("nombre_entidad"),
        trim(col("Nit Entidad")).alias("id_nit_entidad"),
        trim(col("Departamento")).alias("nombre_depto_entidad"),
        trim(col("Ciudad")).alias("nombre_ciudad_datos"),
        trim(col("Localización")).alias("nombre_localizacion_datos"),
        trim(col("Orden")).alias("tipo_orden_territorial_entidad"),
        trim(col("Sector")).alias("nombre_sector_entidad"),
        trim(col("Rama")).alias("nombre_rama_entidad"),
        trim(col("Entidad Centralizada")).alias("tipo_centralizada"),
        trim(col("Proceso de Compra")).alias("id_proceso_compra"),
        trim(col("ID Contrato")).alias("id_contrato"),
        trim(col("Referencia del Contrato")).alias("id_ref_contrato"),
        trim(col("Estado Contrato")).alias("tipo_estado_contrato"),
        trim(col("Codigo de Categoria Principal")).alias("id_codigo_cat_principal"),
        trim(col("Descripcion del Proceso")).alias("desc_proceso"),
        trim(col("Tipo de Contrato")).alias("tipo_contrato"),
        trim(col("Modalidad de Contratacion")).alias("tipo_modalidad_contrato"),
        trim(col("Justificacion Modalidad de Contratacion")).alias("desc_justificacion_mod_cont"),
        to_date(trim(col("Fecha de Firma")), "dd/MM/yyyy").alias("fecha_firma"),
        to_date(trim(col("Fecha de Inicio del Contrato")), "dd/MM/yyyy").alias("fecha_inicio_contrato"),
        to_date(trim(col("Fecha de Fin del Contrato")), "dd/MM/yyyy").alias("fecha_fin_contrato"),
        to_date(trim(col("Fecha de Inicio de Ejecucion")), "dd/MM/yyyy").alias("fecha_inicio_ejecucion"),
        to_date(trim(col("Fecha de Fin de Ejecucion")), "dd/MM/yyyy").alias("fecha_fin_ejecucion"),
        trim(col("Condiciones de Entrega")).alias("tipo_condicion_entrega"),
        trim(col("TipoDocProveedor")).alias("tipo_identificacion_proveedor"),
        trim(col("Documento Proveedor")).alias("id_proveedor"),
        upper(trim(col("Proveedor Adjudicado"))).alias("nombre_proveedor"),
        trim(col("Es Grupo")).alias("tipo_marca_grupo"),
        trim(col("Es Pyme")).alias("tipo_marca_pyme"),
        trim(col("Habilita Pago Adelantado")).alias("tipo_habilita_pago_adel"),
        trim(col("Liquidación")).alias("tipo_liquidacion"),
        trim(col("Obligación Ambiental")).alias("tipo_obligacion_ambiental"),
        trim(col("Obligaciones Postconsumo")).alias("tipo_obligacion_postconsumo"),
        trim(col("Reversion")).alias("tipo_reversion"),
        trim(col("Origen de los Recursos")).alias("id_origen_recurso"),
        trim(col("Destino Gasto")).alias("id_destino_gasto"),
        trim(col("Valor del Contrato")).cast("decimal(30,3)").alias("monto_contrato"),
        trim(col("Valor de pago adelantado")).cast("decimal(30,3)").alias("monto_pago_adelanto"),
        trim(col("Valor Facturado")).cast("decimal(30,3)").alias("monto_facturado"),
        trim(col("Valor Pendiente de Pago")).cast("decimal(30,3)").alias("monto_pendiente_pago"),
        trim(col("Valor Pagado")).cast("decimal(30,3)").alias("monto_pagado"),
        trim(col("Valor Amortizado")).cast("decimal(30,3)").alias("monto_amortizado"),
        trim(col("Valor Pendiente de Amortizacion")).cast("decimal(30,3)").alias("monto_pendiente_amorti"),
        trim(col("Valor Pendiente de Ejecucion")).cast("decimal(30,3)").alias("monto_pendiente_ejecucion"),
        trim(col("Estado BPIN")).alias("id_estado_bpin"),
        trim(col("Código BPIN")).alias("id_bpin"),
        trim(col("Anno BPIN")).alias("fecha_anio_bpin"),
        trim(col("Saldo CDP")).cast("decimal(30,3)").alias("monto_saldo_cdp"),
        trim(col("Saldo Vigencia")).cast("decimal(30,3)").alias("monto_saldo_vigencia"),
        trim(col("EsPostConflicto")).alias("tipo_marca_postconflicto"),
        trim(col("Días Adicionados")).alias("numero_adicion_dias"),
        trim(col("Puntos del Acuerdo")).alias("tipo_puntos_acuerdo"),
        trim(col("Pilares del Acuerdo")).alias("id_pilares_acuerdo"),
        trim(col("URLProceso")).alias("desc_url_proceso"),
        upper(trim(col("Nombre Representante Legal"))).alias("nombre_representante_legal"),
        upper(trim(col("Nacionalidad Representante Legal"))).alias("nombre_nacionalidad_rep_leg"),
        trim(col("Domicilio Representante Legal")).alias("desc_direccion_rep_legal"),
        trim(col("Tipo de Identificación Representante Legal")).alias("tipo_id_rep_legal"),
        trim(col("Identificación Representante Legal")).alias("id_nit_rep_legal"),
        trim(col("Género Representante Legal")).alias("tipo_genero_rep_legal"),
        trim(col("Presupuesto General de la Nacion – PGN")).cast("decimal(30,3)").alias("monto_pgn"),
        trim(col("Sistema General de Participaciones")).cast("decimal(30,3)").alias("monto_gral_participantes"),
        trim(col("Sistema General de Regalías")).cast("decimal(30,3)").alias("monto_gral_regalias"),
        trim(col("Recursos Propios (Alcaldías, Gobernaciones y Resguardos Indígenas)")).cast("decimal(30,3)").alias(
            "monto_recursos_territorios"),
        trim(col("Recursos de Credito")).cast("decimal(30,3)").alias("monto_recursos_credito"),
        trim(col("Recursos Propios")).cast("decimal(30,3)").alias("monto_recursos_propios"),
        to_timestamp(trim(col("FechaModificacion")), "dd/MM/yyyy HH:mm:ss").alias("fecha_modificacion"),
        trim(col("Codigo Entidad")).alias("id_entidad"),
        trim(col("Codigo Proveedor")).alias("id_proveedor_secop"),
        lit(date_data).cast("date").alias("fecha_corte_datos")
    )

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
