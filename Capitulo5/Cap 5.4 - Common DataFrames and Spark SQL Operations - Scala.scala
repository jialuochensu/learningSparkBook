// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.appName("cap5.4").getOrCreate()

// COMMAND ----------

//filePath
val delaysPath = "/FileStore/tables/departuredelays.csv"
val airportsPath = "/FileStore/tables/airport_codes_na.txt"

// COMMAND ----------

//airports data set
val airports = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", "\t")
  .csv(airportsPath)
airports.createOrReplaceTempView("airports_na")

// COMMAND ----------

import org.apache.spark.sql.functions._
val delays = spark.read
  .option("header", "true")
  .csv(delaysPath)
  .withColumn("delay", expr("CAST(delay as INT) as delay"))
  .withColumn("distance", expr("CAST(distance as INT) as distance"))
delays.createOrReplaceTempView("departureDelays")

// COMMAND ----------

//temp small table
val foo = delays.filter(
  expr("origin == 'SEA' AND destination == 'SFO' AND date like '01010%' AND delay > 0")
)

foo.createOrReplaceTempView("foo")

// COMMAND ----------

spark.sql("SELECT * FROM foo").show()

// COMMAND ----------

spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

// COMMAND ----------

spark.sql("SELECT * FROM airports_na LIMIT 10").show()

// COMMAND ----------

//union
val bar = delays.union(foo)
bar.createOrReplaceTempView("bar")
bar.filter(expr("origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0")).show()

// COMMAND ----------

foo.join(airports.as('air),
  $"air.IATA" === $"origin"
        ).select("City", "State", "date", "delay", "distance", "destination").show()

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS departureDelaysWindow;
// MAGIC 
// MAGIC CREATE TABLE departureDelaysWindow AS
// MAGIC SELECT origin, destination, SUM(delay) AS TotalDelays
// MAGIC FROM departureDelays
// MAGIC WHERE origin IN ('SEA', 'SFO', 'JFK') 
// MAGIC   AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
// MAGIC GROUP BY origin, destination;
// MAGIC 
// MAGIC SELECT * FROM departureDelaysWindow

// COMMAND ----------

spark.sql("""
SELECT origin, destination, TotalDelays, rank
 FROM (
 SELECT origin, destination, TotalDelays, dense_rank()
 OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
 FROM departureDelaysWindow
 ) t
 WHERE rank <= 3
""").show()


// COMMAND ----------

//Modifications
foo.show()

// COMMAND ----------

//adding new columns
import org.apache.spark.sql.functions.expr
val foo2 = foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))
foo2.show()

// COMMAND ----------

//drop a column
val foo3 = foo2.drop("delay")
foo3.show()

// COMMAND ----------

//rename column
val foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
// MAGIC FROM departureDelays
// MAGIC WHERE origin = 'SEA';

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM (
// MAGIC SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
// MAGIC  FROM departureDelays WHERE origin = 'SEA'
// MAGIC )
// MAGIC PIVOT (
// MAGIC  CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
// MAGIC  FOR month IN (1 JAN, 2 FEB)
// MAGIC )
// MAGIC ORDER BY destination

// COMMAND ----------


