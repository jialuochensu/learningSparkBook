# Databricks notebook source
#Connect PostgreSQL - PLANTILLA

# Read Option 1: Loading data from a JDBC source using load method
jdbcDF1 = (spark
           .read
           .format("jdbc")
           .option("url", "jdbc:postgresql://[DBSERVER]")
           .option("dbtable", "[SCHEMA].[TABLENAME]")
           .option("user", "[USERNAME]")
           .option("password", "[PASSWORD]")
           .load())


# COMMAND ----------

# Read Option 2: Loading data from a JDBC source using jdbc method
jdbcDF2 = (spark
 .read
 .jdbc("jdbc:postgresql://[DBSERVER]", "[SCHEMA].[TABLENAME]", properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))

# COMMAND ----------

# Write Option 1: Saving data to a JDBC source using save method
(jdbcDF1
 .write
 .format("jdbc")
 .option("url", "jdbc:postgresql://[DBSERVER]")
 .option("dbtable", "[SCHEMA].[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .save())

# COMMAND ----------

# Write Option 2: Saving data to a JDBC source using jdbc method
(jdbcDF2
 .write
 .jdbc("jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]",
 properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))

# COMMAND ----------

#Connect MySQL - PLANTILLA

# Loading data from a JDBC source using load
jdbcDF = (spark
 .read
 .format("jdbc")
 .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
 .option("driver", "com.mysql.jdbc.Driver")
 .option("dbtable", "[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .load())

# COMMAND ----------

# Saving data to a JDBC source using save
(jdbcDF
 .write
 .format("jdbc")
 .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
 .option("driver", "com.mysql.jdbc.Driver")
 .option("dbtable", "[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .save())


# COMMAND ----------

# Connect Azure Cosmos DB - PLANTILLA


# Loading data from Azure Cosmos DB
# Read configuration
query = "SELECT c.colA, c.coln FROM c WHERE c.origin = 'SEA'"
readConfig = {
 "Endpoint" : "https://[ACCOUNT].documents.azure.com:443/",
 "Masterkey" : "[MASTER KEY]",
 "Database" : "[DATABASE]",
 "preferredRegions" : "Central US;East US2",
 "Collection" : "[COLLECTION]",
 "SamplingRatio" : "1.0",
 "schema_samplesize" : "1000",
 "query_pagesize" : "2147483647",
 "query_custom" : query
}
# Connect via azure-cosmosdb-spark to create Spark DataFrame
df = (spark
 .read
 .format("com.microsoft.azure.cosmosdb.spark")
 .options(**readConfig)
 .load())
# Count the number of flights
df.count()
# Saving data to Azure Cosmos DB
# Write configuration
writeConfig = {
"Endpoint" : "https://[ACCOUNT].documents.azure.com:443/",
"Masterkey" : "[MASTER KEY]",
"Database" : "[DATABASE]",
"Collection" : "[COLLECTION]",
"Upsert" : "true"
}
# Upsert the DataFrame to Azure Cosmos DB
(df.write
 .format("com.microsoft.azure.cosmosdb.spark")
 .options(*

# COMMAND ----------

#Connect MS SQL Server - PLANTILLA

# Configure jdbcUrl
jdbcUrl = "jdbc:sqlserver://[DBSERVER]:1433;database=[DATABASE]"
# Loading data from a JDBC source
jdbcDF = (spark
 .read
 .format("jdbc")
 .option("url", jdbcUrl)
 .option("dbtable", "[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .load())
# Saving data to a JDBC source
(jdbcDF
 .write
 .format("jdbc")
 .option("url", jdbcUrl)
 .option("dbtable", "[TABLENAME]")
 .option("user", "[USERNAME]")
 .option("password", "[PASSWORD]")
 .save())

