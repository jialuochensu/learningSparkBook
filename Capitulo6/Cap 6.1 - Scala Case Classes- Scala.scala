// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("cap6.1").getOrCreate()
val filePath = "/FileStore/tables/blogs.json"

// COMMAND ----------

//Scala case class
case class Bloggers(id: BigInt, first: String, last: String, url: String, published: String, hits: BigInt, campaigns: Array[String])

// COMMAND ----------

//read file
val bloggerDS = spark
  .read
  .format("json")
  .option("path", filePath)
  .load()
  .as[Bloggers]

// COMMAND ----------

//Creating Sample Data
import scala.util.Random._

case class Usage(uid: Int, uname:String, usage: Int)
val r = new scala.util.Random(42)

val data = for(i<-0 to 1000)
  yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))

val dsUsage = spark.createDataset(data)
dsUsage.show(10)

// COMMAND ----------

//High order functions and functional programming
import org.apache.spark.sql.functions._
dsUsage
  .filter(i => i.usage>900)
  .orderBy(desc("usage"))
  .show(5, false)

// COMMAND ----------

//Otra forma de hacerlo
def auxFiltro(u: Usage) = u.usage>900

dsUsage
  .filter(auxFiltro(_))
  .orderBy(desc("usage"))
  .show(5, false)

// COMMAND ----------

// Use an if-then-else lambda expression and compute a value
dsUsage
  .map(u => {if (u.usage>750) 
               u.usage*0.15 
            else
               u.usage*0.50})
  .show(5, false)

// COMMAND ----------

// Define a function to compute the usage
def auxCostUsage(usage: Int): Double = {
  if (usage>750)
    usage*0.15
  else
    usage*0.50
}
dsUsage
  .map(u=> {auxCostUsage(u.usage)})
  .show(5, false)

// COMMAND ----------

//Añadir una nueva columna
case class UsageCost(uid:Int, uname:String, usage:Int, cost:Double)

def computeUserCostUsage(u: Usage): UsageCost = {
  val v = if(u.usage>750)
            u.usage*0.15
          else
            u.usage*0.50
  UsageCost(u.uid, u.uname, u.usage, v)
}

dsUsage.map(u=>computeUserCostUsage(u)).show(5)

// COMMAND ----------


