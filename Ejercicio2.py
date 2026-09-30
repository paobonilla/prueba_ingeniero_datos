# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
import os

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

def merge_orders(
    spark: SparkSession,
    new_batch_df: DataFrame,
    target_table_path: str,
    customers_path: str = "data/silver/customers_silver",
) -> DataFrame:
  """     
  Hace merge incremental de nuevas órdenes a la tabla Silver.
  new_batch_df: order_id_txt, customer_id, pais_cd, monto_dec, updated_ts
  """
  
  target = DeltaTable.forPath(spark, target_table_path)
  
  #Parte 1: Deduplicacion para no duplicar datos
  customers_df = spark.read.format("delta").load(customers_path)
  window_spec = Window.partitionBy("customer_id").orderBy(
    F.col("ingesta_ts").desc()
  )

  customers_dedup = (
    customers_df.withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .select(
      F.col("customer_id").alias("c_id"),
      F.col("nombre_txt").alias("customer_nombre_txt"),
      F.col("email_txt").alias("customer_email_txt")
    )
  )

  #Parte 2: Particionar y enriquecer
  enriched_df = (
    new_batch_df.join(
      customers_dedup,
      new_batch_df.customer_id == customers_dedup.c_id,
      "left"
    )
    .drop("c_id")
    .withColumn("updated_dt", F.to_date(F.col("updated_ts")))
  )

  #Parte 3: Merge con order_id y pais_cd
  merge_keys = ("t.order_id_txt = s.order_id_txt AND t.pais_cd = s.pais_cd")

  (target.alias("t")
    .merge(enriched_df.alias("s"), merge_keys)         
    .whenMatchedUpdateAll()         
    .whenNotMatchedInsertAll()         
    .execute()     
  )   

  # Parte 4: Particionamiento por pais y fecha
  (enriched_df.write.format("delta")
    .mode("append")
    .partitionBy("pais_cd", "updated_dt")
    .save(target_table_path + "_backup")
  )

  return enriched_df

#1. Se inicia la sesion de Spark con soporte Delta Lake
builder = (
  SparkSession.builder.appName("Ejercicio2")
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

target_silver_path = "data/silver/customer_purchases_silver"

#2. se carga tabla silver
existing_purchases_df = spark.read.format("delta").load(target_silver_path)
new_order = (
  spark.range(1)
  .select(
    F.lit("ORD-999").alias("order_id_txt"),
    F.lit("60c1f2a1e4b0a1a2b3c4d5e7").alias("customer_id"),
    F.lit("SV").alias("pais_cd"),
    F.lit(75.00).cast(DecimalType(12, 2)).alias("monto_dec"),
    F.lit("USD").alias("moneda_cd"),
    F.current_date().alias("creado_dt"),
    F.current_timestamp().alias("updated_ts")
  )
)

#3 se toma una orden existente y se modifica el monto para hacer un UPDATE
update_batch = (
  existing_purchases_df.filter(F.col("order_id_txt") == "ORD-001")
  .withColumn("monto_dec", (F.col("monto_dec") + 50.00).cast(DecimalType(12, 2)))
  .withColumn("updated_ts", F.current_timestamp())
)

#4 Lote incremental combinado un UPDATE y un INSERT
new_batch_df = update_batch.unionByName(new_order)

print("NEW BATCH A PROCESAR")
new_batch_df.show(truncate=False)

#6. Ejecuta el merge
print("\nEJECUTANDO merge_orders")
resultado = merge_orders(spark, new_batch_df, target_silver_path)

print("\n TABLA SILVER ACTUALIZADA DESPUES DEL MERGE")
spark.read.format("delta").load(target_silver_path).show(truncate=False)

print("\n TABLA GENERADA")
spark.read.format("delta").load(target_silver_path + "_backup").show(truncate=False)