# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver - ETL into a Silver table
# MAGIC 
# MAGIC We need to perform some transformations on the data to move it from bronze to silver tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step Configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration_jsonmoive

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Files in the Bronze Paths

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the rawDF DataFrame
# MAGIC 
# MAGIC **Note**: Use the function `read_batch_raw` to ingest the newly arrived

# COMMAND ----------

rawDF = read_batch_raw("/FileStore/tables/")
rawDF.show(10)

# COMMAND ----------

transformedRawDF = transform_raw(rawDF)
display(transformedRawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema with an Assertion

# COMMAND ----------

from pyspark.sql.types import *

assert transformedRawDF.schema == StructType(
    [
        StructField("Movies", StringType(), True),
        StructField("Ingesttime", TimestampType(), False),
        StructField("Status", StringType(), False),
        StructField("p_Ingestdate", DateType(), False),
    ]
)
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Batch to a Bronze Table
# MAGIC 
# MAGIC **Note**: you will need to begin the write with the `.save()` method on
# MAGIC your writer.
# MAGIC 
# MAGIC           Use the function `batch_writer` to ingest the newly arrived
# MAGIC 
# MAGIC **Be sure to partition on `p_ingestdate`**.

# COMMAND ----------

rawToBronzeWriter = batch_writer(
    dataframe=transformedRawDF, partition_column="p_ingestdate"
)

rawToBronzeWriter.save(bronzePath)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movies_df_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(Status) from movies_df_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze to Silver Step
# MAGIC 
# MAGIC Let's start the Bronze to Silver step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load New Records from the Bronze Records
# MAGIC 
# MAGIC **EXERCISE**
# MAGIC 
# MAGIC Load all records from the Bronze table with a status of `"new"`.

# COMMAND ----------

bronzeDF = spark.read.table("movies_df_bronze").filter("Status = 'New'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract the Nested JSON from the Bronze Records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Extract the Nested JSON from the `value` column
# MAGIC **EXERCISE**
# MAGIC 
# MAGIC Use `pyspark.sql` functions to extract the `"value"` column as a new
# MAGIC column `"nested_json"`.

# COMMAND ----------

bronzeDF.show()

# COMMAND ----------

bronzeDF.count()

# COMMAND ----------

bronzeDF.dtypes

# COMMAND ----------

from pyspark.sql.functions import from_json

json_schema = """
    Id LONG,
    Title STRING,      
    Overview STRING,
    OriginalLanguage STRING,
    Price DOUBLE,
    ReleaseDate TIMESTAMP,
    Budget DOUBLE,
    Revenue DOUBLE,
    RunTime LONG,
    Tagline STRING,
    genres ARRAY<STRING>,
    CreatedBy STRING,
    CreatedDate TIMESTAMP,
    UpdatedBy STRING,
    UpdatedDate TIMESTAMP,
    ImdbUrl STRING,
    TmdbUrl STRING,
    PosterUrl STRING,
    BackdropUrl STRING
"""

bronzeAugmentedDF = bronzeDF.withColumn("nested_json", from_json(col("Movies"), json_schema))

# COMMAND ----------

bronzeAugmentedDF.show()

# COMMAND ----------

bronzeAugmentedDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create the Silver DataFrame by Unpacking the `nested_json` Column
# MAGIC 
# MAGIC Unpacking a JSON column means to flatten the JSON and include each top level attribute
# MAGIC as its own column.
# MAGIC 
# MAGIC 🚨 **IMPORTANT** Be sure to include the `"Movies"` column in the Silver DataFrame
# MAGIC because we will later use it as a unique reference to each record in the
# MAGIC Bronze table

# COMMAND ----------

movies_df_silver = bronzeAugmentedDF.select("Movies", "nested_json.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema with an Assertion

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

assert movies_df_silver.schema == _parse_datatype_string(
    """
    Movies STRING,
    Id LONG,
    Title STRING,      
    Overview STRING,
    OriginalLanguage STRING,
    Price DOUBLE,
    ReleaseDate TIMESTAMP,
    Budget DOUBLE,
    Revenue DOUBLE,
    RunTime LONG,
    Tagline STRING,
    genres ARRAY<STRING>,
    CreatedBy STRING,
    CreatedDate TIMESTAMP,
    UpdatedBy STRING,
    UpdatedDate TIMESTAMP,
    ImdbUrl STRING,
    TmdbUrl STRING,
    PosterUrl STRING,
    BackdropUrl STRING
"""
)
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform the Data
# MAGIC 
# MAGIC 💪🏼 Remember that we name the new column `p_eventdate` to indicate
# MAGIC that we are partitioning on this column.
# MAGIC 
# MAGIC 🕵🏽‍♀️ Remember that we are keeping the `Movies` as a unique reference to values
# MAGIC in the Bronze table.

# COMMAND ----------

from pyspark.sql.functions import col

movies_df_silver = movies_df_silver.select(
    'Movies',
    'Id',
    'Title',
    'Overview',
    'OriginalLanguage',
    'Price',
    'ReleaseDate',
    year(col('ReleaseDate').cast('date')).alias('p_ReleaseYear'),
    'Budget',
    'Revenue',
    'RunTime',
    'Tagline',
    'genres',
    'CreatedBy',
    'CreatedDate',
    'UpdatedBy',
    'UpdatedDate',
    'ImdbUrl',
    'TmdbUrl',
    'PosterUrl',
    'BackdropUrl'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema with an Assertion
# MAGIC 
# MAGIC 💪🏼 Remember, the function `_parse_datatype_string` converts a DDL format schema string into a Spark schema.

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

assert movies_df_silver.schema == _parse_datatype_string(
    """
    Movies STRING,
    Id LONG,
    Title STRING,      
    Overview STRING,
    OriginalLanguage STRING,
    Price DOUBLE,
    ReleaseDate TIMESTAMP,
    p_ReleaseYear INTEGER,
    Budget DOUBLE,
    Revenue DOUBLE,
    RunTime LONG,
    Tagline STRING,
    genres ARRAY<STRING>,
    CreatedBy STRING,
    CreatedDate TIMESTAMP,
    UpdatedBy STRING,
    UpdatedDate TIMESTAMP,
    ImdbUrl STRING,
    TmdbUrl STRING,
    PosterUrl STRING,
    BackdropUrl STRING
"""
), "Schemas do not match"
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine the Bad Data
# MAGIC 
# MAGIC Recall that at step, `00_ingest_raw`, we identified that some records were coming in
# MAGIC with device_ids passed as uuid strings instead of string-encoded integers.
# MAGIC Our Silver table stores device_ids as integers so clearly there is an issue
# MAGIC with the incoming data.
# MAGIC 
# MAGIC In order to properly handle this data quality issue, we will quarantine
# MAGIC the bad records for later processing.

# COMMAND ----------

# MAGIC %md
# MAGIC Check for records that have nulls - compare the output of the following two cells

# COMMAND ----------

movies_df_silver.count()

# COMMAND ----------

movies_df_silver.na.drop().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split the Silver DataFrame

# COMMAND ----------

movies_df_silver_clean = movies_df_silver.filter("Id IS NOT NULL AND ReleaseDate IS NOT NULL AND RUNTIME > 0")
movies_df_silver_quarantine = movies_df_silver.filter("Id IS NULL OR ReleaseDate IS NULL OR RUNTIME < 0")

# COMMAND ----------

movies_df_silver_clean = movies_df_silver_clean.dropDuplicates()

# COMMAND ----------

movies_df_silver_clean.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Quarantined Records

# COMMAND ----------

movies_df_silver_quarantine = movies_df_silver_quarantine.dropDuplicates()

# COMMAND ----------

display(movies_df_silver_quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Clean Batch to a Silver Table
# MAGIC 
# MAGIC **EXERCISE:** Batch write `movies_df_silver_clean` to the Silver table path, `silverPath`.
# MAGIC 
# MAGIC 1. Use format, `"delta"`
# MAGIC 1. Use mode `"append"`.
# MAGIC 1. Do **NOT** include the `Movies` column.
# MAGIC 1. Partition by `"p_Releasedate"`.

# COMMAND ----------

(
    movies_df_silver_clean.select(
    'Id',
    'Title',
    'Overview',
    'OriginalLanguage',
    'Price',
    'ReleaseDate',
    'p_ReleaseYear',
    'Budget',
    'Revenue',
    'RunTime',
    'Tagline',
    'genres',
    'CreatedBy',
    'CreatedDate',
    'UpdatedBy',
    'UpdatedDate',
    'ImdbUrl',
    'TmdbUrl',
    'PosterUrl',
    'BackdropUrl'
    )
    .write.format("delta")
    .mode("append")
    .partitionBy("p_ReleaseYear")
    .save(silverPath)
)

# COMMAND ----------

display(dbutils.fs.ls(silverPath))

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movies_df_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movies_df_silver
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema with an Assertion

# COMMAND ----------

silverTable = spark.read.table("movies_df_silver")
expected_schema = """
    Id LONG,
    Title STRING,      
    Overview STRING,
    OriginalLanguage STRING,
    Price DOUBLE,
    ReleaseDate TIMESTAMP,
    p_ReleaseYear INTEGER,
    Budget DOUBLE,
    Revenue DOUBLE,
    RunTime LONG,
    Tagline STRING,
    genres ARRAY<STRING>,
    CreatedBy STRING,
    CreatedDate TIMESTAMP,
    UpdatedBy STRING,
    UpdatedDate TIMESTAMP,
    ImdbUrl STRING,
    TmdbUrl STRING,
    PosterUrl STRING,
    BackdropUrl STRING
"""

assert silverTable.schema == _parse_datatype_string(
    expected_schema
), "Schemas do not match"
print("Assertion passed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movies_df_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Bronze table to Reflect the Loads
# MAGIC 
# MAGIC **EXERCISE:** Update the records in the Bronze table to reflect updates.
# MAGIC 
# MAGIC ### Step 1: Update Clean records
# MAGIC Clean records that have been loaded into the Silver table and should have
# MAGIC    their Bronze table `status` updated to `"loaded"`.
# MAGIC 
# MAGIC 💃🏽 **Hint** You are matching the `Movies` column in your clean Silver DataFrame
# MAGIC to the `Movies` column in the Bronze table.

# COMMAND ----------

from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = movies_df_silver_clean.withColumn("status", lit("loaded"))

update_match = "bronze.Movies = clean.Movies"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **EXERCISE:** Update the records in the Bronze table to reflect updates.
# MAGIC 
# MAGIC ### Step 2: Update Quarantined records
# MAGIC Quarantined records should have their Bronze table `status` updated to `"quarantined"`.
# MAGIC 
# MAGIC 🕺🏻 **Hint** You are matching the `value` column in your quarantine Silver
# MAGIC DataFrame to the `value` column in the Bronze table.

# COMMAND ----------

silverAugmented = movies_df_silver_quarantine.withColumn(
    "status", lit("quarantined")
)

update_match = "bronze.Movies = quarantine.Movies"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

movies_df_silver_clean.count()

# COMMAND ----------


