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
SELECT DISTINCT (Branch_ID) AS Branch_ID , BranchName
from parquet.`abfss://silver@project00.dfs.core.windows.net/carsales`''')
df_src.display()

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    df_sink = spark.sql('''
    SELECT dim_branch_key,Branch_ID,BranchName
    FROM cars_catalog.gold.dim_branch''')

else:
    df_sink = spark.sql('''
    SELECT 1 AS dim_branch_key,Branch_ID,BranchName
    FROM parquet.`abfss://silver@project00.dfs.core.windows.net/carsales`
    where 1=0''') #  create empty table with the header in the initial run




# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE SURROGATE KEY

# COMMAND ----------

if (incremental_flag == '0'):
 max_value = 1
else:
    max_value_df = spark.sql('''
    SELECT max(dim_branch_key) as max FROM cars_catalog.gold.dim_branch ''')
    max_value=max_value_df.collect()[0][0]+1
  

# COMMAND ----------

df_filter_new = df_src.withColumn("dim_branch_key", max_value+monotonically_increasing_id())
df_filter_new.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_branch

# COMMAND ----------

from delta.tables import *


# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@project00.dfs.core.windows.net/dim_branch")
    delta_tbl.alias("trg").merge(
        df_filter_new.alias("src"),
        "trg.dim_branch_key = src.dim_branch_key") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll().execute()

else:
    df_filter_new.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path","abfss://gold@project00.dfs.core.windows.net/dim_branch").saveAsTable("cars_catalog.gold.dim_branch")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_branch 