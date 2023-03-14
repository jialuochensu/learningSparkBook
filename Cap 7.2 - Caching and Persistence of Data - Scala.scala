// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("Cap7.2").getOrCreate()

// COMMAND ----------

//metodo cache
val df = spark.range(1*10000000).toDF("id").withColumn("square", $"id"*$"id")
df.cache() //cachea los datos
df.count() //materializamos el cache

// COMMAND ----------

//a continuacion vemos como el tiempo se reduce drásticamente
df.count()

// COMMAND ----------

//De 21.03sec a 1.25sec
//Tener en cuenta que cuando usas el cache() o persist(), no estan totalmente cacheados hasta que invoques una accion y DEPENDIENDO que accion puede cachear más o menos, p.e take(1), solo cacheara 1 particion por el catalizador que cree que no necesita computar todas las particiones restantes

// COMMAND ----------

//metodo persist
import org.apache.spark.storage.StorageLevel

//Create a DF with 10M records
val df1 = spark.range(1*10000000).toDF("id").withColumn("square", $"id"*$"id")
df1.persist(StorageLevel.DISK_ONLY) //Serialize the data and cache it on disk
df1.count() //Materializamos

// COMMAND ----------

df1.count()

// COMMAND ----------

df1.createOrReplaceTempView("dfTable")
spark.sql("CACHE TABLE dfTable")
spark.sql("SELECT count(*) FROM dfTable").show()

// COMMAND ----------

//Sort merged join
import scala.util.Random

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

var states = scala.collection.mutable.Map[Int, String]()
var items = scala.collection.mutable.Map[Int, String]()
val rnd = new scala.util.Random(42)

// COMMAND ----------

// Initialize states and items purchased
states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4", 5-> "SKU-5")

// COMMAND ----------

// Create DataFrames
val usersDF = (0 to 1000000)
  .map(id => (id, s"user_${id}", s"user_${id}@databricks.com", states(rnd.nextInt(5))))
 .toDF("uid", "login", "email", "user_state")
val ordersDF = (0 to 1000000)
 .map(r => (r, r, rnd.nextInt(10000), 10 * r* 0.2d, states(rnd.nextInt(5)), items(rnd.nextInt(5))))
 .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

// COMMAND ----------

val usersOrdersDF = ordersDF.join(usersDF, $"users_id" === $"uid")

usersOrdersDF.show(false)

// COMMAND ----------

usersOrdersDF.explain()

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS OrdersTbl

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS UsersTbl

// COMMAND ----------

//optimizing shuffle sort merfe join
import org.apache.spark.sql.functions._

//save as managed tables by bucketing in parquet format
usersDF.orderBy(asc("uid"))
  .write
  .format("parquet")
  .bucketBy(8, "uid")
  .saveAsTable("UsersTbl")

ordersDF.orderBy(asc("users_id"))
  .write
  .format("parquet")
  .bucketBy(8, "users_id")
  .saveAsTable("OrdersTbl")


// COMMAND ----------

//cache the tables
spark.sql("CACHE TABLE UsersTbl")
spark.sql("CACHE TABLE OrdersTbl")

//Read them back
val usersBucketDF = spark.table("UsersTbl")
val ordersBucketDF = spark.table("OrdersTbl")

//join
val joinUsersOrdersBucketDF = ordersBucketDF.join(usersBucketDF, $"users_id"===$"uid")

joinUsersOrdersBucketDF.show(false)

// COMMAND ----------

joinUsersOrdersBucketDF.explain()

// COMMAND ----------

+-*9
