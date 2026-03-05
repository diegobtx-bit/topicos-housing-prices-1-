#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark para exportar la tabla final (Gold) a CSV
Proyecto: California Housing Prices
"""

import os
from pyspark.sql import SparkSession

# 1. Crear sesión Spark con soporte Hive
spark = SparkSession.builder \
    .appName("Export_Gold_Housing_To_CSV-DiegoFlores") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. Configuración de origen (Capa Functional)
database = "topicosb_functional"
table = "housing_enriched"

print(f"📥 Leyendo tabla {database}.{table}...")

# 3. Leer tabla Hive
try:
    df = spark.table(f"{database}.{table}")
    
    # 4. Ruta Dinámica (Funciona en Mac y Linux)
    # Detectamos la carpeta actual del proyecto
    cwd = os.getcwd()
    output_path = f"file:{cwd}/datalake/temp"

    # 5. Guardar como CSV
    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", "true") \
      .option("delimiter", ",") \
      .csv(output_path)

    print(f"✅ Exportación completada en: {output_path}")

except Exception as e:
    print(f"❌ Error al exportar: {str(e)}")

finally:
    spark.stop()