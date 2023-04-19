import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
from datetime import date


def transform_data(spark, key, date_origin, source_bucket, destination_bucket):
    data_source = spark.read.option("multiline", "true").json(f"s3://{source_bucket}/{key}/{key}_{date_origin}.json")
    date_data = date.today()

    # to_date(trim(col("fecha_de_publicacion_fase")), "yyyy-mm-dd").alias("fecha_planeacion_pre"),
    # to_date(trim(col("fecha_de_publicacion_fase_1")), "yyyy-mm-dd").alias("fecha_seleccion_pre"),
    data_transform = data_source.select(
        trim(col("entidad")).alias("nombre_entidad"),
        trim(col("nit_entidad")).alias("id_nit_entidad"),
        trim(col("departamento_entidad")).alias("nombre_depto_entidad"),
        trim(col("ciudad_entidad")).alias("nombre_ciudad_entidad"),
        trim(col("ordenentidad")).alias("tipo_orden_territorial_entidad"),
        trim(col("codigo_pci")).alias("tipo_centralizada"),
        trim(col("id_del_proceso")).alias("id_proceso"),
        trim(col("referencia_del_proceso")).alias("id_referencia"),
        trim(col("ppi")).alias("id_unidad_pci"),
        trim(col("id_del_portafolio")).alias("id_portafolio"),
        trim(col("nombre_del_procedimiento")).alias("nombre_contrato"),
        trim(col("descripci_n_del_procedimiento")).alias("desc_contrato"),
        trim(col("fase")).alias("tipo_fase"),
        to_date(trim(col("fecha_de_publicacion_del")), "yyyy-mm-dd").alias("fecha_publicacion"),
        to_date(trim(col("fecha_de_ultima_publicaci")), "yyyy-mm-dd").alias("fecha_actualizacion"),
        to_date(trim(col("fecha_de_publicacion")), "yyyy-mm-dd").alias("fecha_manifestacion_int"),
        to_date(trim(col("fecha_de_publicacion_fase_2")), "yyyy-mm-dd").alias("fecha_fase_borrador"),
        to_date(trim(col("fecha_de_publicacion_fase_3")), "yyyy-mm-dd").alias("fecha_seleccion"),
        trim(col("precio_base")).cast("decimal(30,3)").alias("monto_precio_base"),
        trim(col("modalidad_de_contratacion")).alias("tipo_contratacion"),
        trim(col("justificaci_n_modalidad_de")).alias("desc_modalidad_contrato"),
        trim(col("duracion")).alias("cantidad_duracion"),
        trim(col("unidad_de_duracion")).alias("tipo_duracion"),
        to_date(trim(col("fecha_de_recepcion_de")), "yyyy-mm-dd").alias("fecha_recep_resp"),
        to_date(trim(col("fecha_de_apertura_de_respuesta")), "yyyy-mm-dd").alias("fecha_apert_resp"),
        to_date(trim(col("fecha_de_apertura_efectiva")), "yyyy-mm-dd").alias("fecha_apert_efect"),
        trim(col("ciudad_de_la_unidad_de")).alias("nombre_ciudad_unidad"),
        trim(col("nombre_de_la_unidad_de")).alias("nombre_unidad_contr"),
        trim(col("proveedores_invitados")).alias("cantidad_prov_invitados"),
        trim(col("proveedores_con_invitacion")).alias("cantidad_porv_directos"),
        trim(col("visualizaciones_del")).alias("cantidad_visualizaciones"),
        trim(col("proveedores_que_manifestaron")).alias("cantidad_prov_interesados"),
        trim(col("respuestas_al_procedimiento")).alias("cantidad_resp_proced"),
        trim(col("respuestas_externas")).alias("cantidad_resp_externas"),
        trim(col("conteo_de_respuestas_a_ofertas")).alias("cantidad_resp_direct"),
        trim(col("proveedores_unicos_con")).alias("cantidad_prov_unicos"),
        trim(col("numero_de_lotes")).alias("cantidad_lotes"),
        trim(col("estado_del_procedimiento")).alias("tipo_estado_proced"),
        trim(col("id_estado_del_procedimiento")).alias("id_estado_proced"),
        trim(col("adjudicado")).alias("tipo_respuesta_adjudicado"),
        trim(col("id_adjudicacion")).alias("id_adjudicado"),
        trim(col("codigoproveedor")).alias("id_proveedor_secop"),
        trim(col("departamento_proveedor")).alias("nombre_depto_prov"),
        trim(col("ciudad_proveedor")).alias("nombre_ciudad_prov"),
        to_date(trim(col("fecha_adjudicacion")), "yyyy-mm-dd").alias("fecha_adjudicacion"),
        trim(col("valor_total_adjudicacion")).cast("decimal(30,3)").alias("monto_total_adjudicado"),
        trim(col("nombre_del_adjudicador")).alias("nombre_usuario_adjor"),
        upper(trim(col("nombre_del_proveedor"))).alias("nombre_proveedor"),
        trim(col("nit_del_proveedor_adjudicado")).alias("id_nit_proveedor"),
        trim(col("codigo_principal_de_categoria")).alias("id_codigo_categoria"),
        trim(col("estado_de_apertura_del_proceso")).alias("tipo_estado_apertura"),
        trim(col("tipo_de_contrato")).alias("tipo_contrato"),
        trim(col("subtipo_de_contrato")).alias("tipo_sub_contrato"),
        trim(col("categorias_adicionales")).alias("tipo_cat_ad"),
        trim(col("urlproceso").cast("string")).alias("desc_url_proceso"),
        trim(col("codigo_entidad")).alias("id_entidad"),
        trim(col("estadoresumen")).alias("tipo_estado_resumen"),
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
