# Databricks notebook source
import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# COMMAND ----------

#Build a SparkSession using SparkSessionAPI
spark = (SparkSession
         .builder
         .appName("PythonMnMCount")
         .getOrCreate()
        )

# COMMAND ----------

#Get the M&M dataset filename
file = "/FileStore/tables/mnm_dataset.csv"
#Read the file into a Spark DataFrame using the CSV
mnm_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(file)
)

# COMMAND ----------

mnm_df.show()

# COMMAND ----------

# 1. Select from the DataFrame the fields "State", "Color", and "Count"
# 2. Since we want to group each state and its M&M color count, we use groupBy()
# 3. Aggregate counts of all colors and groupBy() State and Color
# 4 orderBy() in descending order

count_mnm_df = (
    mnm_df
    .select("State", "Color", "Count")
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total_Contado"))
    .orderBy("Total_Contado", ascending = False)
)

count_mnm_df.show(n=60, truncate=False)




# COMMAND ----------

print("Total rows = %d" % (count_mnm_df.count()))

# COMMAND ----------

# While the above code aggregated and counted for all
# the states, what if we just want to see the data for
# a single state, e.g., CA?
# 1. Select from all rows in the DataFrame
# 2. Filter only CA state
# 3. groupBy() State and Color as we did above
# 4. Aggregate the counts for each color
# 5. orderBy() in descending order
# Find the aggregate count for California by filtering

ca_count_mnm_df = (
    mnm_df
    .select("State", "Color", "Count")
    .where(mnm_df.State == "CA")
    .groupBy("State", "Color")
    .agg(count("Color").alias("Total_Contado"))
    .orderBy("Total_Contado", ascending = False)
)
ca_count_mnm_df.show(n=10, truncate = False)

# COMMAND ----------

#spark.stop()

# COMMAND ----------


