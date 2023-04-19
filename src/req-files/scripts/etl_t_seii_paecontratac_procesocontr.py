import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
from datetime import date


def transform_data(spark, key, date_origin, source_bucket, destination_bucket):
    data_source = spark.read.option("multiline", "true").json(f"s3://{source_bucket}/{key}/{key}_{date_origin}.json")

    date_data = date.today()

    data_transform = data_source.select(
        trim(col("Entidad")).alias("nombre_entidad"),
        trim(col("Nit Entidad")).alias("id_nit_entidad"),
        trim(col("Ciudad Entidad")).alias("nombre_ciudad_entidad"),
        trim(col("OrdenEntidad")).alias("tipo_orden_territorial_entidad"),
        trim(col("Entidad Centralizada")).alias("tipo_centralizada"),
        trim(col("ID del Proceso")).alias("id_proceso"),
        trim(col("Referencia del Proceso")).alias("id_referencia"),
        trim(col("PCI")).alias("id_unidad_pci"),
        trim(col("ID del Portafolio")).alias("id_portafolio"),
        trim(col("Nombre del Procedimiento")).alias("nombre_contrato"),
        trim(col("Descripción del Procedimiento")).alias("desc_contrato"),
        trim(col("Fase")).alias("tipo_fase"),
        to_date(trim(col("Fecha de Publicacion del Proceso")), "MM/dd/yyyy").alias("fecha_publicacion"),
        to_date(trim(col("Fecha de Ultima Publicación")), "MM/dd/yyyy").alias("fecha_actualizacion"),
        to_date(trim(col("Fecha de Publicacion (Fase Planeacion Precalificacion)")), "MM/dd/yyyy").alias(
            "fecha_planeacion_pre"),
        to_date(trim(col("Fecha de Publicacion (Fase Seleccion Precalificacion)")), "MM/dd/yyyy").alias(
            "fecha_seleccion_pre"),
        to_date(trim(col("Fecha de Publicacion (Manifestacion de Interes)")), "MM/dd/yyyy").alias(
            "fecha_manifestacion_int"),
        to_date(trim(col("Fecha de Publicacion (Fase Borrador)")), "MM/dd/yyyy").alias("fecha_fase_borrador"),
        to_date(trim(col("Fecha de Publicacion (Fase Seleccion)")), "MM/dd/yyyy").alias("fecha_seleccion"),
        trim(col("Precio Base")).cast("decimal(30,3)").alias("monto_precio_base"),
        trim(col("Modalidad de Contratacion")).alias("tipo_contratacion"),
        trim(col("Justificación Modalidad de Contratación")).alias("desc_modalidad_contr"),
        trim(col("Duracion")).alias("cantidad_duracion"),
        trim(col("Unidad de Duracion")).alias("tipo_duracion"),
        to_date(trim(col("Fecha de Recepcion de Respuestas")), "MM/dd/yyyy").alias("fecha_recep_resp"),
        to_date(trim(col("Fecha de Apertura de Respuesta")), "MM/dd/yyyy").alias("fecha_apert_resp"),
        to_date(trim(col("Fecha de Apertura Efectiva")), "MM/dd/yyyy").alias("fecha_apert_efect"),
        trim(col("Ciudad de la Unidad de Contratación")).alias("nombre_ciudad_unidad"),
        trim(col("Nombre de la Unidad de Contratación")).alias("nombre_unidad_contr"),
        trim(col("Proveedores Invitados")).alias("cantidad_prov_invitados"),
        trim(col("Proveedores con Invitacion Directa")).alias("cantidad_porv_directos"),
        trim(col("Visualizaciones del Procedimiento")).alias("cantidad_visualizaciones"),
        trim(col("Proveedores que Manifestaron Interes")).alias("cantidad_prov_interesados"),
        trim(col("Respuestas al Procedimiento")).alias("cantidad_resp_proced"),
        trim(col("Respuestas Externas")).alias("cantidad_resp_externas"),
        trim(col("Conteo de Respuestas a Ofertas")).alias("cantidad_resp_direct"),
        trim(col("Proveedores Unicos con Respuestas")).alias("cantidad_prov_unicos"),
        trim(col("Numero de Lotes")).alias("cantidad_lotes"),
        trim(col("Estado del Procedimiento")).alias("tipo_estado_proced"),
        trim(col("ID Estado del Procedimiento")).alias("id_estado_proced"),
        trim(col("Adjudicado")).alias("tipo_respuesta_adjudicado"),
        trim(col("ID Adjudicacion")).alias("id_adjudicado"),
        trim(col("CodigoProveedor")).alias("id_proveedor_secop"),
        trim(col("Departamento Proveedor")).alias("nombre_depto_prov"),
        trim(col("Ciudad Proveedor")).alias("nombre_ciudad_prov"),
        to_date(trim(col("Fecha Adjudicacion")), "MM/dd/yyyy").alias("fecha_adjudicacion"),
        trim(col("Valor Total Adjudicacion")).cast("decimal(30,3)").alias("monto_total_adjudicado"),
        trim(col("Nombre del Adjudicador")).alias("nombre_usuario_adjor"),
        trim(col("Nombre del Proveedor Adjudicado")).alias("nombre_proveedor"),
        trim(col("NIT del Proveedor Adjudicado")).alias("id_nit_proveedor"),
        trim(col("Codigo Principal de Categoria")).alias("id_codigo_categoria"),
        trim(col("Estado de Apertura del Proceso")).alias("tipo_estado_apertura"),
        trim(col("Tipo de Contrato")).alias("tipo_contrato"),
        trim(col("Subtipo de Contrato")).alias("tipo_sub_contrato"),
        trim(col("Categorias Adicionales")).alias("tipo_cat_ad"),
        trim(col("URLProceso")).alias("desc_url_proceso"),
        lit(date_data).cast("date").alias("fecha_corte_datos")
    )

    data_transform.write.mode('overwrite').partitionBy("fecha_corte_datos", "tipo_estado_apertura",
                                                       "tipo_fase").parquet(f"s3://{destination_bucket}/{key}")
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
