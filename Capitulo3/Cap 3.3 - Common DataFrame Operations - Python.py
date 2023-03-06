# Databricks notebook source
#Using DataFrameReader and DataFrameWriter
filePath = "/FileStore/tables/sf_fire_calls.csv"

# COMMAND ----------

from pyspark.sql.types import *

#Programmatic way to define a schema
fire_schema = StructType([
    StructField("CallNumber", IntegerType(), True),
    StructField("UnitID", StringType(), True),
    StructField("IncidentNumber", IntegerType(), True),
    StructField("CallType", StringType(), True),
    StructField("CallDate", StringType(), True),
    StructField("WatchDate", StringType(), True),
    StructField("CallFinalDisposition", StringType(), True),
    StructField("AvailableDtTm", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Zipcode", IntegerType(), True),
    StructField("Battalion", StringType(), True),
    StructField("StationArea", StringType(), True),
    StructField("Box", StringType(), True),
    StructField("OriginalPriority", StringType(), True),
    StructField("Priority", StringType(), True),
    StructField("FinalPriority", IntegerType(), True),
    StructField("ALSUnit", BooleanType(), True),
    StructField("CallTypeGroup", StringType(), True),
    StructField("NumAlarms", IntegerType(), True),
    StructField("UnitType", StringType(), True),
    StructField("UnitSequenceInCallDispatch", IntegerType(), True),
    StructField("FirePreventionDistrict", StringType(), True),
    StructField("SupervisorDistrict", StringType(), True),
    StructField("Neighborhood", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("RowID", StringType(), True),
    StructField("Delay", FloatType(), True)
    
])

#Read from a CSV file with DataFrameReader
fire_df = spark.read.csv(filePath, header=True, schema=fire_schema)



# COMMAND ----------

fire_df.show(10)

# COMMAND ----------

#Saving DF as a Parquet file 
parquetPath = "/FileStore/tables/firePython"
fire_df.write.format("parquet").save(parquetPath)

# COMMAND ----------

#Saving DF as a Table
parquetTable = "firePythonTabla"
fire_df.write.format("parquet").saveAsTable(parquetTable)

# COMMAND ----------

from pyspark.sql.functions import col 
#Transformations and actions
few_fire_df = (
    fire_df
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType") != "Medical Incident")
)
few_fire_df.show(5, False)

# COMMAND ----------

from pyspark.sql.functions import *
res = (
  fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .agg(countDistinct(col("CallType")).alias("DistinctCallTypes"))
 .show())

# COMMAND ----------

(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .distinct()
 .show())


# COMMAND ----------

#withColumnRenamed() -> Change the name of column
newFire_df = (fire_df
    .withColumnRenamed("Delay", "ResponseDelayedMins"))

(newFire_df
  .select("ResponseDelayedMins")
  .where(col("ResponseDelayedMins")>5)
  .show(5, False))


# COMMAND ----------

#Date-time -> timestamp
fire_ts_df = (newFire_df
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn("OnWatchDate", to_timestamp(col("watchDate"), "MM/dd/yyyy"))
    .drop("watchDate")
    .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
    .drop("AvailableDtTm")
)



# COMMAND ----------

(fire_ts_df
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, False))


# COMMAND ----------

#ejemplo
(fire_ts_df
 .select(year("IncidentDate"))
 .distinct()
 .orderBy(year("IncidentDate"))
 .show()
)

# COMMAND ----------

#most common types of fire calls
(fire_ts_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending = False)
 .show(5, False)
)

# COMMAND ----------

import pyspark.sql.functions as F
(fire_ts_df
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedMins"),F.min("ResponseDelayedMins"), F.max("ResponseDelayedMins"))
 .show()
)
         

# COMMAND ----------


