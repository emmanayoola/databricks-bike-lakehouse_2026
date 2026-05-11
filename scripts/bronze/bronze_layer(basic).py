# Databricks notebook source
# MAGIC %md #Read raw csv files into bronze layer delta tables
# MAGIC

# COMMAND ----------

# MAGIC %md ## Load CRM files

# COMMAND ----------

# custo_info.csv
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/baraa_projectwork/bronze/raw_source_systems/source_crm/cust_info.csv", header=True)
df.write.mode("overwrite").saveAsTable("baraa_projectwork.bronze.crm_cust_info")

# COMMAND ----------

# prd_info.csv
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/baraa_projectwork/bronze/raw_source_systems/source_crm/prd_info.csv", header=True)
df.write.mode("overwrite").saveAsTable("baraa_projectwork.bronze.crm_prd_info")

# COMMAND ----------

# sales_details
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/baraa_projectwork/bronze/raw_source_systems/source_crm/sales_details.csv", header=True)
df.write.mode("overwrite").saveAsTable("baraa_projectwork.bronze.crm_sales_details")

# COMMAND ----------

# MAGIC %md ## Load ERP files

# COMMAND ----------

# CUST_AZ12.csv
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/baraa_projectwork/bronze/raw_source_systems/source_erp/CUST_AZ12.csv", header=True)
df.write.mode("overwrite").saveAsTable("baraa_projectwork.bronze.erp_cust_az12")

# COMMAND ----------

# LOC_A101.csv
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/baraa_projectwork/bronze/raw_source_systems/source_erp/LOC_A101.csv", header=True)
df.write.mode("overwrite").saveAsTable("baraa_projectwork.bronze.erp_loc_a101")

# COMMAND ----------

# PX_CAT_G1V2.csv
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/baraa_projectwork/bronze/raw_source_systems/source_erp/PX_CAT_G1V2.csv", header=True)
df.write.mode("overwrite").saveAsTable("baraa_projectwork.bronze.erp_px_cat_g1v2")
