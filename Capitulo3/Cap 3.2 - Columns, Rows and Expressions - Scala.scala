// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
//En caso de que queramos usar un JSON en vez de datos estáticos: 
val filePath = "/FileStore/tables/blogs.json"
val spark = SparkSession
  .builder
  .appName("Ejemplo_3.7")
  .getOrCreate()

//Creamos el schema, pero esta vez el schema programmatically
//IMPORTANTE -> El nombre tiene que coincidir con los atributos de JSON
val schema = StructType(Array(
  StructField("Id", IntegerType, false),
  StructField("First", StringType, false),
  StructField("Last", StringType, false),
  StructField("Url", StringType, false),
  StructField("Published", StringType, false),
  StructField("Hits", IntegerType, false),
  StructField("Campaigns", ArrayType(StringType), false)
))

//Creamos el DF desde el fichero JSON con el schema
val resDF = spark.read.schema(schema).json(filePath)
resDF.show(false)

//print the schema
println(resDF.printSchema)
println(resDF.schema)

// COMMAND ----------

//operaciones con columnas
import org.apache.spark.sql.functions._
resDF.columns


// COMMAND ----------

//acceder una columna en particular
val accId = resDF.col("Id")

// COMMAND ----------

//usar expresion para computar un valor
resDF.select(expr("Hits * 2").alias("dobleHits")).show(2)
//equivalente
resDF.select((col("Hits")*2).alias("dobleHits")).show(2)

// COMMAND ----------

//Añadir una nueva columna al DF
resDF.withColumn("Big Hitters", (col("Hits")>10000)).show()

// COMMAND ----------

//Concatenar 3 columnas, crear una nueva columna y show
resDF
  .withColumn("AuthorsId", (concat(col("First"), col("Last"), col("Id"))))
  .select(col("AuthorsId"))
  .show(4)

// COMMAND ----------

//Ordenadr por columna ID en orden descendiente

//Este método devuelve un Column Object
resDF
  .sort(col("Id").desc).show()

// COMMAND ----------

//$nombreColumna -> convierne la columna "nombreColumna" en Column
resDF.sort($"Id" . desc).show()

// COMMAND ----------

//Working with ROWS

import org.apache.spark.sql.Row

//Create a Row
val resRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, Array("twitter", "LinkedIn"))
resRow(0)

// COMMAND ----------

//Se puede usar Row para crear DF. algo rapido para testear
val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
val authorsDF = rows.toDF("Author", "State")
authorsDF.show()
               

// COMMAND ----------


