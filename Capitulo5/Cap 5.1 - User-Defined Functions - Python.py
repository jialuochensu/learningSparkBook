# Databricks notebook source
from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("capitulo5.1")
         .getOrCreate()

)

# COMMAND ----------

#Create a cubed function
from pyspark.sql.types import LongType
def cubed(s):
    return s*s*s

#register UDF
spark.udf.register("cubed", cubed, LongType())
    

# COMMAND ----------

#Generate a temp view
spark.range(1,9).createOrReplaceTempView("udf_test")

# COMMAND ----------

spark.sql("SELECT id, cubed(id) FROM udf_test").show()

# COMMAND ----------

import pandas as pd

#import various pyspark SQL funct including pandas_udf
from pyspark.sql.functions import col, pandas_udf

def cubed_pd(a: pd.Series) -> pd.Series:
    return a*a*a
#create the pandas UDF for the cubed function
cubed_udf = pandas_udf(cubed_pd, returnType=LongType())

# COMMAND ----------

#Create a Pandas Series
x = pd.Series([1,2,3])
print(cubed_pd(x))

# COMMAND ----------

df = spark.range(1,9)
df.select("id", cubed_udf(col("id"))).show()

# COMMAND ----------


