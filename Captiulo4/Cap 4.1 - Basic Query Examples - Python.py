# Databricks notebook source
from pyspark.sql import SparkSession

#create a SparkSession
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate()
)

#path file 
pathFile = "/FileStore/tables/departuredelays.csv"

#read and create a temp view
df = (spark
      .read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(pathFile)
)

df.createOrReplaceTempView("us_delay_flights_tbl")

# COMMAND ----------

df.show(5) #date,delay,distance,origin,destination

# COMMAND ----------

#vuelos con distancia mayor que 1000 millas
spark.sql("SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance>1000 ORDER BY distance DESC").show(10)


# COMMAND ----------

#vuelos entre SFO y ORD con al menos 2h de retraso
spark.sql("SELECT * FROM us_delay_flights_tbl WHERE origin='SFO' AND destination='ORD' AND delay>120 ORDER BY delay DESC").show(10)

# COMMAND ----------

spark.sql("""SELECT delay, origin, destination,
    CASE
        WHEN delay>360 THEN 'Very Long Delay'
        WHEN delay>120 THEN 'Long Delay'
        WHEN delay>60 AND delay<120 THEN 'Short Delay'
        ELSE 'Early'
    END AS Flight_Delays
    FROM us_delay_flights_tbl
    ORDER BY delay DESC
""").show(10)

# COMMAND ----------

#Se podria haber hecho de esta forma tmb
from pyspark.sql.functions import col, desc
a = (df
 .select("distance", "origin", "destination")
 .where(col("distance")>1000)
 .orderBy(desc("distance")).show(10)
)

# COMMAND ----------

#otra opcion equivalente
b = (df.select("distance", "origin", "destination")
 .where("distance > 1000")
 .orderBy("distance", ascending=False).show(10))
a==b

# COMMAND ----------

#Creating a DB instead of using the default one
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

# COMMAND ----------

#managed table
spark.sql("CREATE TABLE managed_us_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

# COMMAND ----------

#otra forma de hacerlo
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(pathFile, schema = schema)
flights_df.write.saveAsTable("managed_us_flights_tbl1")

# COMMAND ----------

#unmanaged table
spark.sql("""CREATE TABLE unmanaged_us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING) USING csv OPTIONS (PATH '/FileStore/tables/departuredelays.csv')""")

# COMMAND ----------

#Create views
df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")

#temporary and global view
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view")

# COMMAND ----------

spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view")

# COMMAND ----------

#drop a view
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

# COMMAND ----------

spark.catalog.listDatabases()

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------

spark.catalog.listColumns("us_delay_flights_tbl")

# COMMAND ----------

#Reading tables into DataFrames
us_flights_df = spark.sql("SELECT * FROM us_delay_flights_tbl")
us_flights_df2 = spark.table("us_delay_flights_tbl")

# COMMAND ----------

us_flights_df

# COMMAND ----------

#read a parquet file
fileParquet = "/FileStore/tables/firePython/"
parquet_df = spark.read.format("parquet").load(fileParquet)

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# COMMAND ----------


