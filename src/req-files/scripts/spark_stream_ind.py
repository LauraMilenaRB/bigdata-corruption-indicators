import psycopg2
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import date


def get_data_frames(spark, list_source, date_origin):
    dict_sources = {}
    for sources in list_source:
        item = sources.split("/")[-1]
        print(f"{sources}/fecha_corte_datos={date_origin}")
        dict_sources[item] = spark.read.parquet(f"{sources}/fecha_corte_datos={date_origin}")
    return dict_sources


def ind_abuso_contratacion(dfPrCon, date_exc, df):
    dfTotal1 = dfPrCon.join(df.select("id_nit_entidad", "id_nit_proveedor", "id_no_contrato"),
                            ["id_nit_entidad", "id_nit_proveedor"], "inner") \
        .select(col("id_no_contrato"), col("id_nit_entidad"), col("monto_total_adjudicado"), col("id_nit_proveedor")) \
        .union(df).filter(~col("id_nit_proveedor").isin("No Definido", "No Adjudicado")) \
        .groupBy(col("id_nit_entidad"), col("id_nit_proveedor"), col("id_no_contrato")).agg(
        count("*").alias("cantidad_asignadas"), sum("monto_total_adjudicado").alias("monto_suma_total_adjudicado")) \
        .orderBy(col("cantidad_asignadas").desc())

    dfFinal = dfTotal1.select(col("id_no_contrato"),
                              lit("Otros indicadores").alias("nombre_grupo_indicador"),
                              lit("Abuso de la contratación").alias("nombre_indicador"),
                              when(col("cantidad_asignadas") >= 3, lit("Si")).otherwise(lit("No")).alias(
                                  "tipo_alerta_irregularidad"),
                              lit(date_exc).cast("date").alias("fecha_ejecucion")
                              )
    return dfFinal


def ind_ofertas_costosas(dfOfPPro, date_exc, df):
    pass


def ind_contratos_prov_inactivos(dfPNPJESAL, date_exc, df):
    dfPNPJESALCancel = dfPNPJESAL.select(col("nombre_razon_social"), col("tipo_identificacion"), col("id_empresa"),
                                         col("id_nit_empresa"), col("id_digito_verificacion"),
                                         col("fecha_ultima_renovacion"), col("fecha_cancelacion"),
                                         col("tipo_estado_matricula")).filter(
        col("tipo_estado_matricula").isin("CANCELADA"))

    dfunion1 = df.alias("contratos").join(dfPNPJESALCancel.alias("camara"),
                                          col("id_nit_proveedor") == col("id_nit_empresa"), "left")
    dfunion2 = df.alias("contratos").join(dfPNPJESALCancel.alias("camara"),
                                          col("id_nit_proveedor") == col("id_empresa"), "left")
    dfTotalCan = dfunion1.union(dfunion2).distinct()

    dfTotal = dfTotalCan.groupBy(col("id_nit_entidad"), col("id_nit_proveedor"), col("id_no_contrato")).count().orderBy(
        col("count").desc())

    dfFinal = dfTotal.select(col("id_no_contrato"),
                             lit("Otros indicadores").alias("nombre_grupo_indicador"),
                             lit("Contratos con proveedores inactivos").alias("nombre_indicador"),
                             when(col("count") >= 2, lit("Si")).otherwise(lit("No")).alias("tipo_alerta_irregularidad"),
                             lit(date_exc).cast("date").alias("fecha_ejecucion")
                             )

    return dfFinal


def ind_contratos_prov_PEP(dfPeP, date_data, df):
    dfFinalpep1 = df.alias("contratos").join(dfPeP.alias("pep").select(lit(1).alias("marca_pep"), "id_nit_pep"),
                                             col("id_nit_proveedor") == col("id_nit_pep"), "left") \
        .withColumn("marca_pep", when(col("marca_pep").isNull(), 0).otherwise(col("marca_pep"))) \
        .groupBy(col("id_no_contrato")).agg(sum(col("marca_pep")).alias("count"))

    dfFinalpep1 = dfFinalpep1.select(col("id_no_contrato"),
                                     lit("Otros indicadores").alias("nombre_grupo_indicador"),
                                     lit("Contratos con proveedores PEP").alias("nombre_indicador"),
                                     when(col("count") >= 1, lit("Si")).otherwise(lit("No")).alias(
                                         "tipo_alerta_irregularidad"),
                                     lit(date_data).cast("date").alias("fecha_ejecucion")
                                     )
    return dfFinalpep1


def ind_contratos_prov_pust_sensibles(dfPuSenCorr, date_data, df):
    dfFinal = df.alias("contratos").join(
        dfPuSenCorr.alias("pep").select(lit(1).alias("marca_pep"), "id_nit_identificacion"),
        col("id_nit_proveedor") == col("id_nit_identificacion"), "left") \
        .withColumn("marca_pep", when(col("marca_pep").isNull(), 0).otherwise(col("marca_pep"))) \
        .groupBy(col("id_no_contrato")).agg(sum(col("marca_pep")).alias("count"))

    dfEscritura26 = dfFinal.select(col("id_no_contrato"),
                                   lit("Otros indicadores").alias("nombre_grupo_indicador"),
                                   lit("Contratos con proveedores con puestos sensibles").alias("nombre_indicador"),
                                   when(col("count") >= 1, lit("Si")).otherwise(lit("No")).alias(
                                       "tipo_alerta_irregularidad"),
                                   lit(date_data).cast("date").alias("fecha_ejecucion")
                                   )
    return dfEscritura26


def ind_contratistas_contratos_cancel(dfCoCa, date_data, df):
    dfProveedoresCancelados = dfCoCa.alias("cancelados").groupBy(col("id_nit_rep_legal")).count().filter(
        ~col("cancelados.id_nit_rep_legal").isin("No Definido"))

    dfFinal = df.alias("contratos") \
        .join(dfProveedoresCancelados.select(lit(1).alias("marca_cancel"), "id_nit_rep_legal"),
              col("contratos.id_nit_proveedor") == col("cancelados.id_nit_rep_legal"), "left") \
        .withColumn("marca_cancel", when(col("marca_cancel").isNull(), 0).otherwise(col("marca_cancel"))) \
        .groupBy(col("id_no_contrato")).agg(sum(col("marca_cancel")).alias("count"))

    dfEscritura26 = dfFinal.select(col("id_no_contrato"),
                                   lit("Indicadores incumplimiento").alias("nombre_grupo_indicador"),
                                   lit("Contratistas con contratos cancelados").alias("nombre_indicador"),
                                   when(col("count") >= 1, lit("Si")).otherwise(lit("No")).alias(
                                       "tipo_alerta_irregularidad"),
                                   lit(date_data).cast("date").alias("fecha_ejecucion")
                                   )
    return dfEscritura26


def ind_contratos_incumplimiento_entregas(dfS2MulSan, date_data, df):
    pass


def ind_inhabilitados_multas(dfS2MulSan, date_data, df):
    df41 = df.alias("contratos") \
        .join(dfS2MulSan.select("nombre_contratista_sancionado", lit(1).alias("marca_multa")).distinct(),
              col("nombre_proveedor") == col("nombre_contratista_sancionado"), "left") \
        .withColumn("marca_multa", when(col("marca_multa").isNull(), 0).otherwise(col("marca_multa"))) \
        .groupBy(col("id_no_contrato")).agg(sum(col("marca_multa")).alias("count"))

    dfEscritura41 = df41.select(col("id_no_contrato"),
                                lit("Indicadores por inhabilidad").alias("nombre_grupo_indicador"),
                                lit("Inhabilitados por multa").alias("nombre_indicador"),
                                when(col("count") >= 1, lit("Si")).otherwise(lit("No")).alias(
                                    "tipo_alerta_irregularidad"),
                                lit(date_data).cast("date").alias("fecha_ejecucion")
                                )
    return dfEscritura41


def ind_inhabilitados_obras_inconclusas(dfPCObraInco, date_data, df):
    df42 = df.alias("contratos") \
        .join(dfPCObraInco.select("nombre_rol_1", lit(1).alias("marca_obras_inc")).filter(
        ~col("nombre_rol_1").isNull()).distinct(), col("nombre_proveedor") == col("nombre_rol_1"), "left") \
        .withColumn("marca_obras_inc", when(col("marca_obras_inc").isNull(), 0).otherwise(col("marca_obras_inc"))) \
        .groupBy(col("id_no_contrato")).agg(sum(col("marca_obras_inc")).alias("count"))

    dfEscritura42 = df42.select(col("id_no_contrato"),
                                lit("Indicadores por inhabilidad").alias("nombre_grupo_indicador"),
                                lit("Inhabilitados por obras inconclusas").alias("nombre_indicador"),
                                when(col("count") >= 1, lit("Si")).otherwise(lit("No")).alias(
                                    "tipo_alerta_irregularidad"),
                                lit(date_data).cast("date").alias("fecha_ejecucion")
                                )

    return dfEscritura42


def ind_inhabilitados_resp_fiscal(dfRespFis, date_data, df):
    df43 = df.alias("contratos") \
        .join(dfRespFis.select("nombre_responsable_fiscal", lit(1).alias("marca_resp_fiscal")).filter(
        ~col("nombre_responsable_fiscal").isNull()).distinct(), ["nombre_responsable_fiscal"], "left") \
        .withColumn("marca_resp_fiscal", when(col("marca_resp_fiscal").isNull(), 0).otherwise(col("marca_resp_fiscal"))) \
        .groupBy(col("id_no_contrato")).agg(sum(col("marca_resp_fiscal")).alias("count"))

    dfEscritura43 = df43.select(col("id_no_contrato"),
                                lit("Indicadores por inhabilidad").alias("nombre_grupo_indicador"),
                                lit("Inhabilitados por responsabilidad fiscal").alias("nombre_indicador"),
                                when(col("count") >= 1, lit("Si")).otherwise(lit("No")).alias(
                                    "tipo_alerta_irregularidad"),
                                lit(date_data).cast("date").alias("fecha_ejecucion")
                                )

    return dfEscritura43


def main():
    spark = SparkSession.builder.appName('test').getOrCreate()
    list_source = ["s3://test-pgr-raw-zone/t_seii_procecotrata_compraadjudi",
                   "s3://test-pgr-raw-zone/t_seii_contracanela_aislamiencon",
                   "s3://test-pgr-raw-zone/t_otro_pernajuesadl_camarcomerci",
                   "s3://test-pgr-raw-zone/t_otro_persexpupoli_sigepperexpo",
                   "s3://test-pgr-raw-zone/t_seii_ofertaproces_procesocompr",
                   "s3://test-pgr-raw-zone/t_otro_puestsensibl_sigeppsscorr",
                   "s3://test-pgr-raw-zone/t_seii_multasysanci_secopiimulsa",
                   "s3://test-pgr-raw-zone/t_paco_registro_obras_inconclusa",
                   "s3://test-pgr-raw-zone/t_paco_responsabilidad_fiscales",
                   "s3://test-pgr-raw-zone/t_seii_ejecucioncon_avancerevses"]

    date_data = date.today()
    data_frames_origin = get_data_frames(spark, list_source, date_data)
    data_source = spark.readStream.parquet(f"s3://test-pgr-raw-zone/t_streaming_contracts/{date_data}/")

    print("read")
    df1 = ind_abuso_contratacion(data_frames_origin["t_seii_procecotrata_compraadjudi"], date_data, data_source)
    #df2 = ind_ofertas_costosas(data_frames_origin["t_seii_ofertaproces_procesocompr"], date_data, data_source)
    df3 = ind_contratos_prov_inactivos(data_frames_origin["t_otro_pernajuesadl_camarcomerci"], date_data, data_source)
    df4 = ind_contratos_prov_PEP(data_frames_origin["t_otro_persexpupoli_sigepperexpo"], date_data, data_source)
    df5 = ind_contratos_prov_pust_sensibles(data_frames_origin["t_otro_puestsensibl_sigeppsscorr"], date_data, data_source)
    df6 = ind_contratistas_contratos_cancel(data_frames_origin["t_seii_contracanela_aislamiencon"], date_data, data_source)
    #df7 = ind_contratos_incumplimiento_entregas(data_frames_origin["t_seii_ejecucioncon_avancerevses"], date_data, data_source)
    df8 = ind_inhabilitados_multas(data_frames_origin["t_seii_multasysanci_secopiimulsa"], date_data, data_source)
    df9 = ind_inhabilitados_obras_inconclusas(data_frames_origin["t_paco_registro_obras_inconclusa"], date_data, data_source)
    df10 = ind_inhabilitados_resp_fiscal(data_frames_origin["t_paco_responsabilidad_fiscales"], date_data, data_source)

    print("inds_f")

    dfs = [df3, df4, df5, df6, df8, df9, df10]

    dfFinal = df1
    for df in dfs:
        print(df.show(1))
        dfFinal = dfFinal.union(df)

    deleted_data_results = "delete from t_result_indicadores_stream;"
    insert_data_results = f"copy t_result_indicadores_stream from 's3://test-pgr-curated-zone/t_result_indicadores_stream/{date_data} " \
                          f"iam_role 'arn:aws:iam::354824231875:role/AmazonRedshift-indicadores-role' format as json 'auto';"

    conn = psycopg2.connect(
        host='amazonredshift-indicadores-cluster-1.c3ss5hvfzcj7.us-east-1.redshift.amazonaws.com',
        port=5439,
        user='user-redshift-admin',
        password='Redshift123',
        database='bd_contracts'
    )

    query = dfFinal \
        .writeStream \
        .outputMode("append") \
        .format(f"json") \
        .option("startingOffsets", "latest") \
        .option("checkpointLocation", f"s3://test-pgr-aws-logs/") \
        .option("path", f"s3://test-pgr-curated-zone/t_result_indicadores_stream/{date_data}/") \
        .start()

    print("insert table")
    cursor = conn.cursor()
    cursor.execute(deleted_data_results)
    conn.commit()
    cursor.execute(insert_data_results)
    conn.commit()
    conn.close()
    print("write")

    query.awaitTermination()


if __name__ == '__main__':
    main()
