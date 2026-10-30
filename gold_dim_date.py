# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
dbutils.widgets.text('incremental_flag','0')
incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING BRANCH DIMENSONAL MODEL

# COMMAND ----------

df_src = spark.sql('''
SELECT DISTINCT (Date_ID) AS Date_ID
from parquet.`abfss://silver@project00.dfs.core.windows.net/carsales`''')
df_src.display()

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    df_sink = spark.sql('''
    SELECT dim_date_key,Date_ID
    FROM cars_catalog.gold.dim_date''')

else:
    df_sink = spark.sql('''
    SELECT 1 AS dim_date_key,Date_ID
    FROM parquet.`abfss://silver@project00.dfs.core.windows.net/carsales`
    where 1=0''') #  create empty table with the header in the initial run




# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtering new records with old

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE SURROGATE KEY

# COMMAND ----------

if (incremental_flag == '0'):
 max_value = 1
else:
    max_value_df = spark.sql('''
    SELECT max(dim_date_key) as max FROM cars_catalog.gold.dim_date ''')
    max_value=max_value_df.collect()[0][0]+1
  

# COMMAND ----------

df_filter_new = df_src.withColumn("dim_date_key", max_value+monotonically_increasing_id())
df_filter_new.createOrReplaceTempView("v_sales_data")

# COMMAND ----------

from delta.tables import *


# COMMAND ----------

spark.sql("drop table if exists cars_catalog.gold.dim_date")

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@project00.dfs.core.windows.net/dim_date")
    delta_tbl.alias("trg").merge(
        df_filter_new.alias("src"),
        "trg.dim_date_key = src.dim_date_key") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll().execute()

else:
    dbutils.fs.rm("abfss://gold@project00.dfs.core.windows.net/dim_date", True)
    df_filter_new.write.format("delta").mode("overwrite").option("path","abfss://gold@project00.dfs.core.windows.net/dim_date").saveAsTable("cars_catalog.gold.dim_date")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_date