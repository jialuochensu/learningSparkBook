// Databricks notebook source
//Connect PostgreSQL - PLANTILLA


//Read option1: loading data from JDBC source using load method
import org.apache.spark.sql.SparkSession
val jdbcDF1 = spark
  .read
  .format("jdbc")
  .option("url", "jdbc:postgresql:[DBSERVER]")
  .option("dbtable", "[SCHEMA].[TABLENAME]")
  .option("user", "[USERNAME]")
  .option("password", "[PASSWORD]")
  .load()

// COMMAND ----------

//Read option2: loading data from JDBC source using JDBC method

//create a connection properties
import java.utils.Properties
val cxnProp = new Properties()
cxnProp.put("user", "[USERNAME]")
cxnProp.put("password", "[PASSWORD]")

//load data using connection properties
val jdbcDF2 = spark
  .read
  .jdbc("jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]", cxnProp)

// COMMAND ----------

//Write Option1: Saving data to a JDBC source using save method
jdbcDF1
  .write
  .format("jdbc")
  .option("url", "jdbc:postgresql:[DBSERVER]")
  .option("dbtable", [SCHEMA].[TABLENAME])
  .option("user", "[USERNAME]")
  .option("password", "[PASSWORD]")
  .save()

// COMMAND ----------

//Write Option2: Saving data to a JDBC source using JDBC method
jdbcDF2
  .write
  .jdbc(s"jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]", cxnProp)

// COMMAND ----------

//Connect MySQL - PLANTILLA

// Loading data from a JDBC source using load
val jdbcDF = spark
 .read
 .format("jdbc")
 .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
 .option("driver", "com.mysql.jdbc.Driver")
 .option("dbtable", "[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .load()


// COMMAND ----------

// Saving data to a JDBC source using save
jdbcDF
 .write
 .format("jdbc")
 .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
 .option("driver", "com.mysql.jdbc.Driver")
 .option("dbtable", "[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .save()


// COMMAND ----------

//Connect Azure Cosmos DB - PLANTILLA

// Import necessary libraries
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config
// Loading data from Azure Cosmos DB
// Configure connection to your collection
val query = "SELECT c.colA, c.coln FROM c WHERE c.origin = 'SEA'"
val readConfig = Config(Map(
 "Endpoint" -> "https://[ACCOUNT].documents.azure.com:443/",
 "Masterkey" -> "[MASTER KEY]",
 "Database" -> "[DATABASE]",
 "PreferredRegions" -> "Central US;East US2;",
 "Collection" -> "[COLLECTION]",
 "SamplingRatio" -> "1.0",
 "query_custom" -> query
))
// Connect via azure-cosmosdb-spark to create Spark DataFrame
val df = spark.read.cosmosDB(readConfig)
df.count
// Saving data to Azure Cosmos DB
// Configure connection to the sink collection
val writeConfig = Config(Map(
 "Endpoint" -> "https://[ACCOUNT].documents.azure.com:443/",
 "Masterkey" -> "[MASTER KEY]",
 "Database" -> "[DATABASE]",
 "PreferredRegions" -> "Central US;East US2;",
 "Collection" -> "[COLLECTION]",
 "WritingBatchSize" -> "100"
))
// Upsert the DataFrame to Azure Cosmos DB
import org.apache.spark.sql.SaveMode
df.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)

// COMMAND ----------

//Connect MS SQL Server - PLANTILLA

// Loading data from a JDBC source
// Configure jdbcUrl
val jdbcUrl = "jdbc:sqlserver://[DBSERVER]:1433;database=[DATABASE]"
// Create a Properties() object to hold the parameters.
// Note, you can create the JDBC URL without passing in the
// user/password parameters directly.
val cxnProp = new Properties()
cxnProp.put("user", "[USERNAME]")
cxnProp.put("password", "[PASSWORD]")
cxnProp.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
// Load data using the connection properties
val jdbcDF = spark.read.jdbc(jdbcUrl, "[TABLENAME]", cxnProp)
// Saving data to a JDBC source
jdbcDF.write.jdbc(jdbcUrl, "[TABLENAME]", cxnProp)
