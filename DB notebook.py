# Databricks notebook source
# MAGIC %md
# MAGIC # Create catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG cars_catalog
# MAGIC MANAGED LOCATION 'abfss://metastorage@project00.dfs.core.windows.net/path/to/managed_storage';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.gold;