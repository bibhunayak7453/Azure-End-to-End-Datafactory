# Databricks notebook source
# MAGIC %md
# MAGIC # Creating fact table

# COMMAND ----------

df_src = spark.sql('''
SELECT *
from parquet.`abfss://silver@project00.dfs.core.windows.net/carsales`''')
df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Reading all the DIMS

# COMMAND ----------

df_silver = spark.sql('''
SELECT *
from parquet.`abfss://silver@project00.dfs.core.windows.net/carsales`''')

# COMMAND ----------

df_dealer=spark.sql("SELECT * from cars_catalog.gold.dim_dealer")
df_branch=spark.sql("SELECT * from cars_catalog.gold.dim_branch")
df_model=spark.sql("SELECT * from cars_catalog.gold.dim_model")
df_date=spark.sql("SELECT * from cars_catalog.gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Bringing keys to Fact table

# COMMAND ----------

df_fact=df_silver.join(df_branch,df_silver.Branch_ID==df_branch.Branch_ID,how='left').join(df_dealer,df_silver.Dealer_ID==df_dealer.Dealer_ID,how='left').join(df_model,df_silver.Model_ID==df_model.Model_ID,how='left').join(df_date,df_silver.Date_ID==df_date.Date_ID,how='left')
df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing fact table

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS cars_catalog.gold.factsales")

# COMMAND ----------

from delta.tables import *

fact_cols = ["dim_branch_key", "dim_Dealer_key", "dim_model_key", "dim_date_key", "Units_sold", "Revenue", "rev_per_unit"]
df_fact_write = df_fact.select(*fact_cols)

if spark.catalog.tableExists('cars_catalog.gold.factsales'):
    deltatable = DeltaTable.forName(spark, 'cars_catalog.gold.factsales')
    deltatable.alias('t').merge(
        df_fact_write.alias('s'),
        't.dim_model_key = s.dim_model_key and t.dim_branch_key = s.dim_branch_key and t.dim_Dealer_key = s.dim_Dealer_key and t.dim_date_key = s.dim_date_key'
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_fact_write.write.format('delta').mode('overwrite').option("path","abfss://gold@project00.dfs.core.windows.net/factsales").saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales

# COMMAND ----------

