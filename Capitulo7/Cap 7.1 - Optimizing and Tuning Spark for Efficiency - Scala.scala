// Databricks notebook source
import org.apache.spark.sql.SparkSession
def printConfigs(session: SparkSession) = {
 // Get conf
 val mconf = session.conf.getAll
 // Print them
 for (k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}\n") }
}


// COMMAND ----------


val spark = SparkSession.builder
 .config("spark.sql.shuffle.partitions", 5000)
 .config("spark.executor.memory", "2g")
 .master("local[*]")
 .appName("SparkConfig")
 .getOrCreate()
printConfigs(spark)
spark.conf.set("spark.sql.shuffle.partitions",
 spark.sparkContext.defaultParallelism)
println(" ****** Setting Shuffle Partitions to Default Parallelism")
printConfigs(spark)



// COMMAND ----------

spark.sql("SET -v").select("key", "value").show(5, false)

// COMMAND ----------

//Cambiar particiones
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 5)

// COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

//especificar un cierto numero de particiones segun el fichero
//val ds = spark.read.textFile("../README.md").repartition(16)
//ds.rdd.getNumPartitions
val numDF = spark.range(1000L * 1000 * 1000).repartition(16)
numDF.rdd.getNumPartitions

// COMMAND ----------


