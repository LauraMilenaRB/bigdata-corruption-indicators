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
        trim(col("nombre_entidad")).alias("nombre_entidad"),
        trim(col("nit_entidad")).alias("id_nit_entidad"),
        trim(col("departamento")).alias("nombre_depto_entidad"),
        trim(col("ciudad")).alias("nombre_ciudad_datos"),
        trim(col("Localizaci_n")).alias("nombre_localizacion_datos"),
        trim(col("orden")).alias("tipo_orden_territorial_entidad"),
        trim(col("sector")).alias("nombre_sector_entidad"),
        trim(col("rama")).alias("nombre_rama_entidad"),
        trim(col("entidad_centralizada")).alias("tipo_centralizada"),
        trim(col("proceso_de_compra")).alias("id_proceso_compra"),
        trim(col("id_contrato")).alias("id_contrato"),
        trim(col("referencia_del_contrato")).alias("id_ref_contrato"),
        trim(col("estado_contrato")).alias("tipo_estado_contrato"),
        trim(col("codigo_de_categoria_principal")).alias("id_codigo_cat_principal"),
        trim(col("descripcion_del_proceso")).alias("desc_proceso"),
        trim(col("tipo_de_contrato")).alias("tipo_contrato"),
        trim(col("modalidad_de_contratacion")).alias("tipo_modalidad_contrato"),
        trim(col("justificacion_modalidad_de")).alias("desc_justificacion_mod_cont"),
        to_date(trim(col("fecha_de_firma")), "yyyy-mm-dd").alias("fecha_firma"),
        to_date(trim(col("fecha_de_inicio_del_contrato")), "yyyy-mm-dd").alias("fecha_inicio_contrato"),
        to_date(trim(col("fecha_de_fin_del_contrato")), "yyyy-mm-dd").alias("fecha_fin_contrato"),
        to_date(trim(col("fecha_de_inicio_de_ejecucion")), "yyyy-mm-dd").alias("fecha_inicio_ejecucion"),
        to_date(trim(col("fecha_de_fin_de_ejecucion")), "yyyy-mm-dd").alias("fecha_fin_ejecucion"),
        trim(col("condiciones_de_entrega")).alias("tipo_condicion_entrega"),
        trim(col("tipodocproveedor")).alias("tipo_identificacion_proveedor"),
        trim(col("documento_proveedor")).alias("id_proveedor"),
        upper(trim(col("proveedor_adjudicado"))).alias("nombre_proveedor"),
        trim(col("es_grupo")).alias("tipo_marca_grupo"),
        trim(col("es_pyme")).alias("tipo_marca_pyme"),
        trim(col("habilita_pago_adelantado")).alias("tipo_habilita_pago_adel"),
        trim(col("liquidaci_n")).alias("tipo_liquidacion"),
        trim(col("obligaci_n_ambiental")).alias("tipo_obligacion_ambiental"),
        trim(col("obligaciones_postconsumo")).alias("tipo_obligacion_postconsumo"),
        trim(col("reversion")).alias("tipo_reversion"),
        trim(col("origen_de_los_recursos")).alias("id_origen_recurso"),
        trim(col("destino_gasto")).alias("id_destino_gasto"),
        trim(col("valor_del_contrato")).cast("decimal(30,3)").alias("monto_contrato"),
        trim(col("valor_de_pago_adelantado")).cast("decimal(30,3)").alias("monto_pago_adelanto"),
        trim(col("valor_facturado")).cast("decimal(30,3)").alias("monto_facturado"),
        trim(col("valor_pendiente_de_pago")).cast("decimal(30,3)").alias("monto_pendiente_pago"),
        trim(col("valor_pagado")).cast("decimal(30,3)").alias("monto_pagado"),
        trim(col("valor_amortizado")).cast("decimal(30,3)").alias("monto_amortizado"),
        trim(col("valor_pendiente_de")).cast("decimal(30,3)").alias("monto_pendiente_amorti"),
        trim(col("valor_pendiente_de_ejecucion")).cast("decimal(30,3)").alias("monto_pendiente_ejecucion"),
        trim(col("estado_bpin")).alias("id_estado_bpin"),
        trim(col("c_digo_bpin")).alias("id_bpin"),
        trim(col("anno_bpin")).alias("fecha_anio_bpin"),
        trim(col("saldo_cdp")).cast("decimal(30,3)").alias("monto_saldo_cdp"),
        trim(col("saldo_vigencia")).cast("decimal(30,3)").alias("monto_saldo_vigencia"),
        trim(col("espostconflicto")).alias("tipo_marca_postconflicto"),
        trim(col("d_as_adicionados")).alias("numero_adicion_dias"),
        trim(col("puntos_del_acuerdo")).alias("tipo_puntos_acuerdo"),
        trim(col("pilares_del_acuerdo")).alias("id_pilares_acuerdo"),
        trim(col("urlproceso").cast("string")).alias("desc_url_proceso"),
        upper(trim(col("nombre_representante_legal"))).alias("nombre_representante_legal"),
        upper(trim(col("nacionalidad_representante"))).alias("nombre_nacionalidad_rep_leg"),
        trim(col("domicilio_representante_legal")).alias("desc_direccion_rep_legal"),
        trim(col("tipo_de_identificaci_n")).alias("tipo_id_rep_legal"),
        trim(col("identificaci_n_representante")).alias("id_nit_rep_legal"),
        trim(col("g_nero_representante_legal")).alias("tipo_genero_rep_legal"),
        trim(col("presupuesto_general_de_la")).cast("decimal(30,3)").alias("monto_pgn"),
        trim(col("sistema_general_de")).cast("decimal(30,3)").alias("monto_gral_participantes"),
        trim(col("sistema_general_de_regal")).cast("decimal(30,3)").alias("monto_gral_regalias"),
        trim(col("recursos_propios_alcald_as")).cast("decimal(30,3)").alias(
            "monto_recursos_territorios"),
        trim(col("recursos_de_credito")).cast("decimal(30,3)").alias("monto_recursos_credito"),
        trim(col("recursos_propios")).cast("decimal(30,3)").alias("monto_recursos_propios"),
        to_timestamp(trim(col("fechamodificacion")), "yyyy-mm-dd'T'HH:mm:ss").alias("fecha_modificacion"),
        trim(col("codigo_entidad")).alias("id_entidad"),
        trim(col("codigo_proveedor")).alias("id_proveedor_secop"),
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
    transform_data(spark, key=pyspark_args.key, date_origin=pyspark_args.date_origin,
                   source_bucket=pyspark_args.staging_bucket,
                   destination_bucket=pyspark_args.raw_bucket
                   )


if __name__ == '__main__':
    main()
