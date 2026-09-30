#Ejercicio 1 de Bronze a Silver
import os

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\bin;" + os.environ.get("PATH", "")

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

#1. Se inicia la sesion de Spark con soporte Delta Lake
builder = (
  SparkSession.builder.appName("Ejercicio1")
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

#2. Lee el archivo JSON multiline y carga el DataFrame Bronze
bronze_df = spark.read.option("multiline", "true").json("data/customers_bronze.json")

#3. Convirtiendo Bronze a Silver
#Parser de fechas de negocio evalua diferentes formatos estandar
business_timestamp = F.coalesce(
  F.expr("try_to_timestamp(created_at, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")"),
  F.expr("try_to_timestamp(created_at, \"yyyy-MM-dd'T'HH:mm:ss\")"),
  F.expr("try_to_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss')")
)

silver_df = (
  bronze_df
  #3.1 Identificador: se renombra _id con el sufijo estandar de llave primaria (_id)
  .withColumn("customer_id", F.col("_id"))
  #3.2 Pais: Se normaliza a mayusculas y se eliminan espacios en blanco
  .withColumn("pais_cd", F.upper(F.trim(F.col("country"))))
  #3.3 Nombre: Se eliminan espacios, si es NULL se da el valor 'DESCONOCIDO'
  .withColumn(
    "nombre_txt",
    F.coalesce(F.trim(F.col("full_name")), F.lit("DESCONOCIDO"))
  )
  #3.4 Correo: Extraccion de struct, se mantiene el NULL
  .withColumn("email_txt", F.col("contact.email"))
  #3.5 Telefonos: si el arreglo esta vacio se da el valor de NULL
  .withColumn(
    "telefonos_arr",
    F.when(F.size(F.col("contact.phones")) > 0, F.col("contact.phones")).otherwise(
      F.lit(None)
    )
  )
  #3.6 Creado fecha: se valida fecha segun formatos, si es NULL se mantiene
  .withColumn("creado_ts", business_timestamp)
  #3.7 Si el dato es NULL se ingresa la fecha de cuando se ingreso el dato
  .withColumn("ingesta_ts", F.current_timestamp())
  #3.8 Fecha de particion: estableciendo el formato de fecha de la particion
  .withColumn(
    "creado_dt", F.coalesce(F.to_date(F.col("creado_ts")), F.to_date(F.col("ingesta_ts")))
  )
  #3.9 Booleano de activo, estandarizando que solo true o 1 retorna True y lo demas es False
  .withColumn(
    "activo_flag",
    F.when(
      F.lower(F.col("is_active").cast("string")).isin("true","1"), True
    ).otherwise(False)
  )
  #3.10 Seleccion de columnas Silver solo se muestran las columnas estandarizadas
  .select(
    "customer_id",
    "pais_cd",
    "nombre_txt",
    "email_txt",
    "telefonos_arr",
    "creado_ts",
    "ingesta_ts",
    "creado_dt",
    "activo_flag"
  )
)

#4. Mostrar DataFrame Silver
silver_df.printSchema()
silver_df.show(truncate=False)

#5. Extraccion de compras
###
customer_purchases_silver = (
  bronze_df
  #5.1 Se mantienen el identificador, pais, fecha de particion
  .withColumn("customer_id", F.col("_id"))
  .withColumn("pais_cd", F.upper(F.trim(F.col("country"))))
  .withColumn(
    "creado_dt",
    F.coalesce(F.to_date(business_timestamp), F.to_date(F.current_timestamp()))
  )
  #5.2 Filtrar clientes que tengan compras
  .filter(
    (F.col("purchases").isNotNull()) & (F.size(F.col("purchases")) > 0)
  )
  #5.3 La matriz cada elemento equivale a una fila
  .select(
    "customer_id",
    "pais_cd",
    "creado_dt",
    F.explode("purchases").alias("p")
  )
  #5.4 Atributos con sufijos estandar y se establece DecimalType para el dinero
  .select(
    F.col("p.order_id").alias("order_id_txt"),
    F.col("customer_id"),
    F.col("pais_cd"),
    F.col("p.amount").cast(DecimalType(12, 2)).alias("monto_dec"),
    F.col("p.currency").alias("moneda_cd"),
    F.col("creado_dt")
  )
  #5.5  Se eliminan registros que tienen monto NULL
  .filter(F.col("monto_dec").isNotNull())
)

#6. Mostrar DataFrame Silver de compras
print("\nTABLA customer_purchases_silver")
customer_purchases_silver.printSchema()
customer_purchases_silver.show(truncate=False)

#7 Particionado por pais_cd y creado_dt
silver_df.write.format("delta").mode("overwrite").partitionBy(
  "pais_cd", "creado_dt"
).save("data/silver/customers_silver")

customer_purchases_silver.write.format("delta").mode("overwrite").partitionBy(
  "pais_cd", "creado_dt"
).save("data/silver/customer_purchases_silver")

print("\n TABLAS DELTA escritas en DATA SILVER")