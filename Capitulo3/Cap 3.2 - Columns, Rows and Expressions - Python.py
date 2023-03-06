# Databricks notebook source
#Como definir un SCHEMA. 
#2 formas: formal y por DDL (Data Definition Language)

# COMMAND ----------

from pyspark.sql import SparkSession
# Create our static data
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter","LinkedIn"]],
     [2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
     [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web", "twitter", "FB","LinkedIn"]],
     [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568,["twitter", "FB"]],
     [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web","twitter", 
                                                                      "FB","LinkedIn"]],
     [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568,["twitter", "LinkedIn"]]
 ]

#Define the schema 
schema = "`Id` INT, `Nombre` STRING, `Apellido` STRING, `Url` STRING, `Publicado` STRING, `Hits` INT, `Campanyas` ARRAY<STRING>"

#Creamos SparkSession
spark = (
    SparkSession
    .builder
    .appName("Ejemplo-3.6")
    .getOrCreate()
)


# COMMAND ----------

#Creamos un DF usando el schema
res_df = spark.createDataFrame(data, schema)
res_df.show()

# COMMAND ----------

print(res_df.printSchema())

# COMMAND ----------

#Working with ROWS
from pyspark.sql import Row
#Create a Row
resRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"])
resRow[1]

# COMMAND ----------

#Se puede usar Row para crear DF. algo rapido para testear
rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])
authors_df.show()


# COMMAND ----------


