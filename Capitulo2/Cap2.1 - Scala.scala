// Databricks notebook source
//package main.scala.chapter2
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// COMMAND ----------

//Build a SparkSession using SparkSessionAPI
val spark = SparkSession
  .builder
  .appName("MnMCount")
  .getOrCreate()
//Get the M&M dataset filename
val file = "/FileStore/tables/mnm_dataset.csv"
//Read the file into a Spark DataFrame using the CSV
val mnmDF = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(file)

// COMMAND ----------

mnmDF.show()

// COMMAND ----------

mnmDF.columns

// COMMAND ----------

// Aggregate counts of all colors and groupBy() State and Color
// orderBy() in descending order
val countMnMDF = mnmDF
  .select("State", "Color", "Count")
  .groupBy("State", "Color")
  .agg(count("Count").alias("Total_contado"))
  .orderBy(desc("Total_contado"))

// COMMAND ----------

//show the result
countMnMDF.show(60)

// COMMAND ----------

println(s"Total Rows = ${countMnMDF.count()}")

// COMMAND ----------

// Aggregate counts of all colors and groupBy() State and Color
// Find the aggregate counts for California by filtering
// orderBy() in descending order
val caCountMnMDF = mnmDF
  .select("State", "Color", "Count")
  .where(col("State") === "CA")
  .groupBy("State", "Color")
  .agg(count("Count").alias("Total_contado"))
  .orderBy(desc("Total_contado"))

// COMMAND ----------

//show the result
caCountMnMDF.show(10)

// COMMAND ----------


