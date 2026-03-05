#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark: Migración de Capa Gold (CSV) a MongoDB
Proyecto: California Housing Prices - Diego Flores
"""

from pyspark.sql import SparkSession

# 1. Configuración de la sesión Spark con el conector de MongoDB
spark = SparkSession.builder \
    .appName("Export_Housing_Mongo-DiegoFlores") \
    .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1:27017") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .getOrCreate()

# 2. Ruta del CSV generado en el paso anterior (Capa Functional)
# Adaptado a tu repositorio actual
csv_path = "file:/home/hadoop/topicos-housing-prices-1-/datalake/temp/part-*.csv"

print(f"📥 Cargando datos desde: {csv_path}")

# 3. Leer el CSV con inferencia de esquema
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_path)

# 4. Debug y validación en consola
print("========== DEBUG DATASET HOUSING ==========")
print(f"Total de registros a migrar: {df.count()}")
df.show(5, truncate=False)
df.printSchema()
print("===========================================")

# 5. Escritura en MongoDB
# Creamos la base de datos 'topicos_housing' y la colección 'housing_gold'
try:
    print("📤 Iniciando persistencia en MongoDB...")
    df.write \
      .format("mongodb") \
      .mode("overwrite") \
      .option("database", "topicos_housing") \
      .option("collection", "housing_gold") \
      .save()
    
    print("✅ ¡Migración a MongoDB completada exitosamente!")

except Exception as e:
    print(f"❌ Error durante la migración: {str(e)}")

finally:
    spark.stop()
