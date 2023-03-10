// Databricks notebook source
import org.apache.spark.sql.types

//Create a simple dataset

val t1 = Array(35, 36, 32, 30, 40, 42, 38)
val t2 = Array(31, 32, 34, 55, 56)
val t_c = Seq(t1, t2).toDF("celsius")
t_c.createOrReplaceTempView("tC")
t_c.show()

// COMMAND ----------

//calculare de fahrenhei from celsius
import org.apache.spark.sql.SparkSession
val spark = SparkSession
  .builder
  .appName("cap5.3")
  .getOrCreate()

//TRANSFORM
spark.sql("SELECT celsius, transform(celsius, t->(((t*9)) div 5)+32) as fahrenheit FROM tC").show()

// COMMAND ----------

//FILTER
spark.sql("SELECT celsius, filter(celsius, t -> (t>38)) as high FROM tC").show()

// COMMAND ----------

//EXISTS
spark.sql("SELECT celsius, exists(celsius, t -> (t=38)) as threshold FROM tC").show()

// COMMAND ----------

//REDUCE
spark.sql("SELECT celsius, reduce(celsius, 0, (t, acc) -> t+acc, acc -> (acc div size(celsius)*9 div 5) +32) as avgFahrenheit FROM tC").show()

// COMMAND ----------


