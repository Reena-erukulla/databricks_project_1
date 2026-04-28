# Databricks notebook source
df_transactions=spark.table("samples.bakehouse.sales_transactions")
df_transactions.show()

# COMMAND ----------

df_suppliers=spark.table("samples.bakehouse.sales_suppliers")
df_suppliers.show()

# COMMAND ----------

df_franchises=spark.table("samples.bakehouse.sales_franchises")
df_franchises.show()

# COMMAND ----------

df_customers=spark.table("samples.bakehouse.sales_customers")
df_customers.show()

# COMMAND ----------

df_customer_reviews=spark.table("samples.bakehouse.media_customer_reviews")
df_customer_reviews.show()

# COMMAND ----------

df_gold_reviews=spark.table("samples.bakehouse.media_gold_reviews_chunked")
df_gold_reviews.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_transactions=df_transactions.withColumn("ingestion_date",current_timestamp())
df_suppliers=df_suppliers.withColumn("ingestion_date",current_timestamp())
df_franchises=df_franchises.withColumn("ingestion_date",current_timestamp())
df_customers=df_customers.withColumn("ingestion_date",current_timestamp())
df_customer_reviews=df_customer_reviews.withColumn("ingestion_date",current_timestamp())
df_gold_reviews=df_gold_reviews.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df_transactions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.sales_transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

display(spark.table("bronze.sales_transactions"))

# COMMAND ----------

df_suppliers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.sales_suppliers")
display(spark.table("bronze.sales_suppliers"))
df_franchises.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.sales_franchises")
display(spark.table("bronze.sales_franchises"))
df_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.sales_customers")
display(spark.table("bronze.sales_customers"))
df_customer_reviews.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.media_customer_reviews")
display(spark.table("bronze.media_customer_reviews"))
df_gold_reviews.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.media_gold_reviews_chunked")
display(spark.table("bronze.media_gold_reviews_chunked"))

# COMMAND ----------

s = spark.table("bronze.sales_transactions").select("transactionID").distinct().count()
print(s)
s = spark.table("bronze.sales_suppliers").select("supplierID").distinct().count()
print(s)
s = spark.table("bronze.sales_franchises").select("franchiseID").distinct().count()
print(s)
s = spark.table("bronze.sales_customers").select("customerID").distinct().count()
print(s)
s = spark.table("bronze.media_customer_reviews").select("new_id").distinct().count()
print(s)
s = spark.table("bronze.media_gold_reviews_chunked").select("chunk_id").distinct().count()
print(s)

# COMMAND ----------

