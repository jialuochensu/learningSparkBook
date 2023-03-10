# Databricks notebook source
from pyspark.sql.types import *

#Create a simple dataset
schema = StructType([StructField("celsius", ArrayType(IntegerType()))])

t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")
t_c.show()

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("cap5.3").getOrCreate()
    
spark.sql("SELECT celsius, transform(celsius, t -> ((t*9) div 5) + 32) as fahrenheit FROM tC").show()

# COMMAND ----------


