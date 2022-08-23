# Databricks notebook source
# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration_jsonmoive

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Files in the Raw Path

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge All Json Files Into One

# COMMAND ----------

files = dbutils.fs.ls('/FileStore/tables/')
for f in files:
    print(f.path)

# COMMAND ----------

#Reading json files
schema_json = 'movie ARRAY<STRING>'
movies = spark.read.option('multiline', 'true').option('inferSchema', 'true').schema(schema_json).json("/FileStore/tables/")
movies_df_raw = movies.withColumn('Movies', explode(col("movie")))
print(type(movies_df_raw))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Display the Raw Data

# COMMAND ----------

print(movies_df_raw.count())
display(movies_df_raw.printSchema())
movies_df_raw.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Ingestion Matadata

# COMMAND ----------

movies_df_raw = (
  movies_df_raw.select(
    "Movies",
    current_timestamp().alias("Ingesttime"),
    lit("New").alias("Status"),
    current_timestamp().cast("date").alias("Ingestdate")
  )
)

# COMMAND ----------

display(movies_df_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Batch to a Bronze Table

# COMMAND ----------

(
  movies_df_raw.select(
    'Ingesttime',
    'Movies',
    'Status',
    col('Ingestdate').alias('p_Ingestdate')
  )
  .write.format('delta')
  .mode('append')
  .partitionBy('p_Ingestdate')
  .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Bronze Table in the Metastore

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS movies_df_bronze
""")

spark.sql(
    f"""
CREATE TABLE movies_df_bronze
USING DELTA
LOCATION "{bronzePath}"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Classic Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movies_df_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Purge Raw File Path

# COMMAND ----------


