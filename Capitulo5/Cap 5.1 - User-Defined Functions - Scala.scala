// Databricks notebook source
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.appName("Capitulo5.1").getOrCreate()
//Create cubed function
val cubed = (s:Long) => {
  s*s*s
}

//Register UDF
spark.udf.register("cubed", cubed)

// COMMAND ----------

//Create a temp view
spark.range(1,9).createOrReplaceTempView("udf_test")

// COMMAND ----------

spark.sql("SELECT id, cubed(id) FROM udf_test").show()

// COMMAND ----------


