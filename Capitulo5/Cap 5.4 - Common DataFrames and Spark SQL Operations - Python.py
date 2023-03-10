# Databricks notebook source
# Set file paths
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

delaysPath = "/FileStore/tables/departuredelays.csv"
airportsPath = "/FileStore/tables/airport_codes_na.txt"

spark = SparkSession.builder.appName("cap5.4").getOrCreate()


# COMMAND ----------

# Obtain airports data set
airportsna = (spark.read
 .format("csv")
 .options(header="true", inferSchema="true", sep="\t")
 .load(airportsPath))
airportsna.createOrReplaceTempView("airports_na")

# Obtain departure delays data set
departureDelays = (spark.read
 .format("csv")
 .options(header="true")
 .load(delaysPath))

departureDelays = (departureDelays
 .withColumn("delay", expr("CAST(delay as INT) as delay"))
 .withColumn("distance", expr("CAST(distance as INT) as distance")))
departureDelays.createOrReplaceTempView("departureDelays")

# Create temporary small table
foo = (departureDelays
 .filter(expr("""origin == 'SEA' and destination == 'SFO' and
 date like '01010%' and delay > 0""")))
foo.createOrReplaceTempView("foo")


# COMMAND ----------

spark.sql("SELECT * FROM foo").show()

# COMMAND ----------

spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

# COMMAND ----------

spark.sql("SELECT * FROM airports_na LIMIT 10").show()

# COMMAND ----------


