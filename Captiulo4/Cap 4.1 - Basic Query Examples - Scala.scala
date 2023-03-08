// Databricks notebook source
import org.apache.spark.sql.SparkSession
//create a spark session
val spark = SparkSession
  .builder
  .appName("SparkSQLExampleApp")
  .getOrCreate()

//path file
val pathFile = "/FileStore/tables/departuredelays.csv"

//Read and create a temporary view
val df = spark
  .read
  .format("csv")
  .option("inferSchema", "true") //nos importa el schema del csv
  .option("header", "true")
  .load(pathFile)

df.createOrReplaceTempView("us_delay_flights_tbl")

// COMMAND ----------

df.show(5) //date,delay,distance,origin,destination

// COMMAND ----------

//vuelos con distancia mayor que 1000 millas
spark.sql("SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance>1000 ORDER BY distance DESC").show(10)

// COMMAND ----------

//test the global temp view from the same clustes, cames from python 
spark.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view")

// COMMAND ----------

spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view")

// COMMAND ----------

//read a parquet file
val fileParquet = "/FileStore/tables/firePython/"
val parquet_df = spark.read.format("parquet").load(fileParquet)

// COMMAND ----------


