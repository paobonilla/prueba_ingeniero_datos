# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
import os, sys
from typing import Any, Dict

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\bin;" + os.environ.get("PATH", "")

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

#diccionario merge
conf: Dict[str, Any]= {
  #Llaves compuestas
  "merge_keys": ["producto_id", "pais_cd"],
  #Columna temporal de control incremental
  "watermark_col": "updated_ts",
  #Columna de particionamiento fisico para el almacenamiento Silver
  "partition_cols": ["pais_cd"],
}

def incremental_load(
  spark: SparkSession,
  batch_df: DataFrame,
  target_table_path: str,
  conf: Dict[str, Any]
) -> None:
  #Ejecuta carga incremental en formato Delta Lake
  #1. Si la tabla no existe, la crea particionada
  #2. Si la tabla existe, ejecuta MERGE respetando el watermark, para evitar sobreescrituras

  merge_keys = conf["merge_keys"]
  watermark_col = conf["watermark_col"]
  partition_cols = conf.get("partition_cols", [])

  #Verificar si existe la tabla Delta en la ruta destino
  table_exists = os.path.exists(target_table_path) and DeltaTable.isDeltaTable(spark, target_table_path)
    
  if not table_exists:
    writer = batch_df.write.format("delta").mode("overwrite")
    if partition_cols:
      writer = writer.partitionBy(*partition_cols)
    writer.save(target_table_path)
    return

  #Si ya existe la tabla
  target = DeltaTable.forPath(spark, target_table_path)
  join_match_cond = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])

  #Solo se actualiza el registro si es el mas reciente
  watermark_cond = f"s.{watermark_col} > t.{watermark_col}"

  (
    target.alias("t")
    .merge(batch_df.alias("s"), join_match_cond)
    .whenMatchedUpdateAll(condition=watermark_cond)
    .whenNotMatchedInsertAll()
    .execute()
  )

builder = (
  SparkSession.builder.appName("Ejercicio3")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config(
    "spark.sql.catalog.spark_catalog",
    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  )
  .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
  .config("spark.hadoop.fs.file.impl.disable.cache", "true")
  .config("spark.driver.extraJavaOptions", "-Dhadoop.native.lib=false")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

target_path = "data/silver/inventory_silver"

#Esquema para inventario
schema = StructType([
  StructField("producto_id", StringType(), False),
  StructField("pais_cd", StringType(), False),
  StructField("stock_int", StringType(), False),
  StructField("updated_ts", StringType(), False)
])

#1. Crear inventory_silver actual
historical_data = [
  ("P-100", "GT", 45, "2024-05-01 08:00:00"),
  ("P-101", "GT", 12, "2024-05-01 08:00:00"),
  ("P-100", "HN", 20, "2024-05-01 08:00:00"),
  ("P-102", "AR", 8, "2024-04-30 17:30:00")
]
hist_df = (
  spark.createDataFrame(historical_data, schema=schema)
  .withColumn("updated_ts", F.to_timestamp("updated_ts"))
)

#Guardar estado base inicial
hist_df.write.format("delta").mode("overwrite").partitionBy("pais_cd").save(target_path)

print("TABLA HISTORICA INICIAL")
spark.read.format("delta").load(target_path).orderBy("pais_cd", "producto_id").show(truncate=False)

#2. Batch entrante inventory_bronze_batch
batch_data = [
  ("P-100", "GT", 40, "2024-05-02 09:10:00"),
  ("P-103", "GT", 5, "2024-05-02 09:12:00"),
  ("P-100", "HN", 20, "2024-04-29 10:00:00"), #Dato antiguo
  ("P-102", "AR", 3, "2024-05-02 10:00:00")
]
batch_df = (
  spark.createDataFrame(batch_data, schema=schema)
  .withColumn("updated_ts", F.to_timestamp("updated_ts"))
)

print("BATCH DE HOY")
batch_df.show(truncate=False)

#3. Ejecutar carga incremental
incremental_load(spark, batch_df, target_path, conf)

print("TABLA inventory_silver despues del MERGE")
spark.read.format("delta").load(target_path).orderBy("pais_cd", "producto_id").show(truncate=False)

spark.stop()