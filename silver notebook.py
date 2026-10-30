# Databricks notebook source
# MAGIC %md
# MAGIC # DATA READING
# MAGIC

# COMMAND ----------

df_raw = spark.read.parquet("abfss://bronze@project00.dfs.core.windows.net/raw_data/")

display(df_raw)

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC #DATA TRANSFORMATION

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.types import *

# 2. Clean whitespace/empty strings and cast types safely
df_silver = df_raw \
    .withColumn("Branch_ID", trim(col("Branch_ID"))) \
    .withColumn("Dealer_ID", trim(col("Dealer_ID"))) \
    .withColumn("Model_ID", trim(col("Model_ID"))) \
    .withColumn("Revenue", trim(col("Revenue")).cast(LongType())) \
    .withColumn("Units_sold", trim(col("Units_sold")).cast(IntegerType())) \
    .withColumn("Date_ID", trim(col("Date_ID"))) \
    .withColumn("Day", trim(col("Day")).cast(IntegerType())) \
    .withColumn("Month", trim(col("Month")).cast(IntegerType())) \
    .withColumn("Year", trim(col("Year")).cast(IntegerType())) \
    .withColumn("BranchName", trim(col("BranchName"))) \
    .withColumn("DealerName", trim(col("DealerName")))

# 3. Optional: Create a clean Spark Date column for analytics
df_silver = df_silver.withColumn(
    "Sales_Date", 
    try_to_date(concat_ws("-", col("Year"), col("Month"), col("Day")), "yyyy-M-d")
)

# Display to verify data is intact
display(df_silver)

# COMMAND ----------

df = df_silver.withColumn('model_category',split(col('Model_ID'), '-').getItem(0))

# COMMAND ----------

df = df.withColumn('rev_per_unit',col('Revenue')/col('Units_sold'))

# COMMAND ----------

# MAGIC %md
# MAGIC #AD-HOC

# COMMAND ----------

ne=df.groupby("Year","BranchName").agg(sum("Units_sold").alias('total_units'))

# COMMAND ----------

ne.filter(col('BranchName') == 'Alpine Motors').display()

# COMMAND ----------

display(ne.sort("Year","total_units",ascending=[1,0]))

# COMMAND ----------

df.write.format('parquet').mode('overwrite').save('abfss://silver@project00.dfs.core.windows.net/carsales')

# COMMAND ----------

ne.write.format('parquet').mode('append').save('abfss://silver@project00.dfs.core.windows.net/data_manupulation')

# COMMAND ----------

# MAGIC %md
# MAGIC #Looking at the silver files

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`abfss://silver@project00.dfs.core.windows.net/carsales`