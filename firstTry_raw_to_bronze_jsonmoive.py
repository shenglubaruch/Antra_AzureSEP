# Databricks notebook source
# MAGIC %md
# MAGIC ##Utility Function

# COMMAND ----------

# MAGIC %run ./includes/utilities_jsonmoive

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge All Json Files Into One

# COMMAND ----------

files = dbutils.fs.ls('/FileStore/tables/')
for f in files:
    print(f.path)

# COMMAND ----------

test = spark.read.option('multiline', 'true').json("/FileStore/tables/movie_0.json")

# COMMAND ----------

display(test)

# COMMAND ----------

display(spark.read.option('inferSchema', 'true').option('multiline', 'true').json('/FileStore/tables/movie_0.json').printSchema())
        


# COMMAND ----------

from pyspark.sql.types import *
jsonSchema = StructType([
    StructField('movie', StringType(), True)
])

display(spark.read
 .schema(jsonSchema)
 .json('/FileStore/tables/movie_0.json')
)

# COMMAND ----------

df0 =spark.read.format('json').load('/FileStore/tables/movie_0.json')
kafka_schema = "value STRING"

raw_health_tracker_data_df = (
  spark.read
  .format('text')
  .schema(kafka_schema)
  .load(rawPath)
)

# COMMAND ----------

display(df0)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Ingestion Matadata

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Batch to a Bronze Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Bronze Table in the Metastore

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Classic Bronze Table
