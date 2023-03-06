// Databricks notebook source
//Using DataFrameReader and DataFrameWriter
val filePath = "/FileStore/tables/sf_fire_calls.csv"

// COMMAND ----------

//En caso de que no apetezca especificar un schema para ver el DF, se podria usar el samplingRatio para ver cierta cantidad de filas.
val sampleDF = spark
 .read
 .option("samplingRatio", 0.001)
 .option("header", true)
 .csv(filePath)
sampleDF.show()

// COMMAND ----------

import org.apache.spark.sql.types._
//Programmatic way to define a schema
val fire_schema = StructType(Array(
    StructField("CallNumber", IntegerType, true),
    StructField("UnitID", StringType, true),
    StructField("IncidentNumber", IntegerType, true),
    StructField("CallType", StringType, true),
    StructField("CallDate", StringType, true),
    StructField("WatchDate", StringType, true),
    StructField("CallFinalDisposition", StringType, true),
    StructField("AvailableDtTm", StringType, true),
    StructField("Address", StringType, true),
    StructField("City", StringType, true),
    StructField("Zipcode", IntegerType, true),
    StructField("Battalion", StringType, true),
    StructField("StationArea", StringType, true),
    StructField("Box", StringType, true),
    StructField("OriginalPriority", StringType, true),
    StructField("Priority", StringType, true),
    StructField("FinalPriority", IntegerType, true),
    StructField("ALSUnit", BooleanType, true),
    StructField("CallTypeGroup", StringType, true),
    StructField("NumAlarms", IntegerType, true),
    StructField("UnitType", StringType, true),
    StructField("UnitSequenceInCallDispatch", IntegerType, true),
    StructField("FirePreventionDistrict", StringType, true),
    StructField("SupervisorDistrict", StringType, true),
    StructField("Neighborhood", StringType, true),
    StructField("Location", StringType, true),
    StructField("RowID", StringType, true),
    StructField("Delay", FloatType, true)
))

//Read from a CSV file with DataFrameReader
val fire_df = spark
  .read
  .schema(fire_schema)
  .option("header", "true")   
  .csv(filePath)
fire_df.show(10)

// COMMAND ----------

//Saving DF as a Parquet file 
val parquetPath = "/FileStore/tables/fireScala"
fire_df.write.format("parquet").save(parquetPath)

// COMMAND ----------

//Saving DF as a Table
val parquetTable = "fireScalaTabla"
fire_df.write.format("parquet").saveAsTable(parquetTable)

// COMMAND ----------

import org.apache.spark.sql.functions._
//Transformations and actions
val few_fire_df = fire_df
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType") =!= "Medical Incident")

few_fire_df.show(5, false)

// COMMAND ----------

fire_df
 .select("CallType")
 .where(col("CallType").isNotNull)
 .agg(countDistinct('CallType) as 'DistinctCallTypes)
 .show()


// COMMAND ----------

fire_df
  .select("CallType")
  .where(col("CallType").isNotNull)
  .distinct()
  .show(10, false)

// COMMAND ----------

//withColumnRenamed -> Change the name of column
val newFire_df = fire_df
  .withColumnRenamed("Delay", "ResponseDelayedMins")

newFire_df
  .select("ResponseDelayedMins")
  .where(col("ResponseDelayedMins")>5)
  .show(5, false)


// COMMAND ----------

// In Scala
val fireTsDF = newFire_df
 .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
 "MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm")
// Select the converted columns
fireTsDF
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, false)


// COMMAND ----------

//ejemplo
fireTsDF
 .select(year(expr("IncidentDate"))) //se podria usar $, col, expr
 .distinct()
 .orderBy(year(expr("IncidentDate")))
 .show()


// COMMAND ----------

//most common types of fire calls
fireTsDF
 .select("CallType")
 .where(col("CallType").isNotNull)
 .groupBy("CallType")
 .count()
 .orderBy(desc("count"))
 .show(5, false)


// COMMAND ----------

import org.apache.spark.sql.{functions => F}
fireTsDF
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedMins"),F.min("ResponseDelayedMins"), F.max("ResponseDelayedMins"))
 .show()


// COMMAND ----------


