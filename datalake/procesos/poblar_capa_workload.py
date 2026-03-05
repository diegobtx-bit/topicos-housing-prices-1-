#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark para despliegue de capa Workload - Proyecto HOUSING
Adaptado para el dataset de precios de viviendas
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# =============================================================================
# @section 1. Configuración de parámetros
# =============================================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description='Proceso de carga - Capa Workload (Housing)')
    parser.add_argument('--env', type=str, default='TopicosB', help='Entorno: DEV, QA, PROD')
    parser.add_argument('--username', type=str, default='hadoop', help='Usuario HDFS')
    parser.add_argument('--base_path', type=str, default='/user', help='Ruta base en HDFS')
    # Nota: En tus instrucciones pasas una ruta de HDFS a este parámetro
    parser.add_argument('--local_data_path', type=str, default='/user/hadoop/dataset', help='Ruta de datos en HDFS')
    return parser.parse_args()

# =============================================================================
# @section 2. Inicialización de SparkSession
# =============================================================================

def create_spark_session(app_name="Carga_Workload_Housing-AldairManosalva"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false") \
        .getOrCreate()

# =============================================================================
# @section 3. Funciones auxiliares (Mantener igual)
# =============================================================================

def crear_database(spark, env, username, base_path):
    db_name = f"{env}_workload".lower()
    db_location = f"{base_path}/{username}/datalake/{db_name}"
    
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_location}'")
    print(f"✅ Database '{db_name}' creada en: {db_location}")
    return db_name

def crear_tabla_external(spark, db_name, table_name, df, location, spark_schema):
    df.createOrReplaceTempView(f"tmp_{table_name}")
    
    # Definición de columnas como STRING para la capa Workload (Raw)
    columnas_sql = ', '.join([f'{field.name} STRING' for field in spark_schema.fields])
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
        {columnas_sql}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\\n'
    STORED AS TEXTFILE
    LOCATION '{location}'
    TBLPROPERTIES('skip.header.line.count'='1')
    """
    spark.sql(create_table_sql)
    
    spark.sql(f"INSERT OVERWRITE TABLE {db_name}.{table_name} SELECT * FROM tmp_{table_name}")
    print(f"✅ Tabla '{db_name}.{table_name}' desplegada en: {location}")

# =============================================================================
# @section 4. Definición de esquema para HOUSING
# =============================================================================

SCHEMAS = {
    "HOUSING": StructType([
        StructField("longitude", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("housing_median_age", StringType(), True),
        StructField("total_rooms", StringType(), True),
        StructField("total_bedrooms", StringType(), True),
        StructField("population", StringType(), True),
        StructField("households", StringType(), True),
        StructField("median_income", StringType(), True),
        StructField("median_house_value", StringType(), True),
        StructField("ocean_proximity", StringType(), True)
    ])
}

# =============================================================================
# @section 5. Proceso de carga
# =============================================================================

def procesar_tabla(spark, args, db_name, table_name, archivo_datos, esquema):
    ruta_input = f"{args.local_data_path}/{archivo_datos}"
    ruta_hdfs_table = f"{args.base_path}/{args.username}/datalake/{db_name}/{table_name.lower()}"
    
    print(f"📥 Procesando: {table_name} | Origen: {ruta_input}")
    
    df = spark.read.csv(
        ruta_input,
        schema=esquema,
        sep='|',  # Usamos pipe como en tu archivo housing.data
        header=True,
        nullValue='NA' # Housing suele usar NA para valores nulos
    )
    
    crear_tabla_external(spark, db_name, table_name, df, ruta_hdfs_table, esquema)
    
    print(f"🔍 Validación {table_name}:")
    spark.sql(f"SELECT * FROM {db_name}.{table_name} LIMIT 5").show()

# =============================================================================
# @section 6. Ejecución principal
# =============================================================================

def main():
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        db_name = crear_database(spark, args.env, args.username, args.base_path)
        
        # Solo procesamos la tabla HOUSING
        tablas_config = [
            {"nombre": "HOUSING", "archivo": "housing.data", "schema": SCHEMAS["HOUSING"]}
        ]
        
        for config in tablas_config:
            procesar_tabla(spark, args, db_name, config["nombre"], config["archivo"], config["schema"])
        
        print("\n🎉 ¡Capa Workload de Housing completada!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()